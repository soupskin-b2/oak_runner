package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Leg is one dispatched worker: a GitHub-hosted machine that registers as an
// ephemeral runner for the source repo and runs exactly one job.
type Leg struct {
	JobID        int64     `json:"job_id"`
	JobName      string    `json:"job_name"`
	Platform     string    `json:"platform"`
	RunnerOS     string    `json:"runner_os"`
	RunnerName   string    `json:"runner_name"`
	RunnerID     int64     `json:"runner_id"`
	Labels       []string  `json:"labels"`
	DispatchedAt time.Time `json:"dispatched_at"`
}

// PendingJob is a queued job on the source repo that we intend to serve.
type PendingJob struct {
	ID       int64     `json:"id"`
	Name     string    `json:"name"`
	Labels   []string  `json:"labels"`
	Platform string    `json:"platform"`
	SeenAt   time.Time `json:"seen_at"`
}

type persistedState struct {
	Pending []*PendingJob `json:"pending"`
	Legs    []*Leg        `json:"legs"`
}

type Controller struct {
	ctx   context.Context
	cfg   *Config
	gh    *GitHub
	proxy *Proxy // nil when the proxy is disabled
	log   *slog.Logger

	mu          sync.Mutex
	pending     map[int64]*PendingJob
	legs        map[int64]*Leg // keyed by job ID
	lastFailure time.Time

	kick chan struct{}
}

func NewController(ctx context.Context, cfg *Config, gh *GitHub, proxy *Proxy, log *slog.Logger) (*Controller, error) {
	c := &Controller{
		ctx: ctx, cfg: cfg, gh: gh, proxy: proxy, log: log,
		pending: map[int64]*PendingJob{},
		legs:    map[int64]*Leg{},
		kick:    make(chan struct{}, 1),
	}
	if err := c.loadState(); err != nil {
		return nil, err
	}
	return c, nil
}

// Run is the single writer: all dispatching happens on this goroutine.
func (c *Controller) Run() {
	t := time.NewTicker(c.cfg.Dispatch.ReconcileInterval.Duration)
	defer t.Stop()
	c.reap()
	c.reconcile()
	for {
		select {
		case <-c.ctx.Done():
			c.mu.Lock()
			c.saveLocked()
			c.mu.Unlock()
			return
		case <-t.C:
			c.reap()
			c.reconcile()
		case <-c.kick:
			c.reconcile()
		}
	}
}

func (c *Controller) trigger() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// --- labels ---------------------------------------------------------------------

// Labels the runner attaches by itself; they must not be sent as custom labels.
var builtinLabels = map[string]bool{
	"self-hosted": true, "linux": true, "windows": true, "macos": true,
	"x64": true, "arm": true, "arm64": true,
}

func lowerSet(labels []string) map[string]bool {
	m := make(map[string]bool, len(labels))
	for _, l := range labels {
		m[strings.ToLower(l)] = true
	}
	return m
}

// platformFor picks the platform a job is meant for, based on its labels.
func (c *Controller) platformFor(jobLabels []string) (*PlatformConfig, []string) {
	have := lowerSet(jobLabels)
	var chosen *PlatformConfig
	for i := range c.cfg.Dispatch.Platforms {
		p := &c.cfg.Dispatch.Platforms[i]
		if have[strings.ToLower(p.Label)] {
			if chosen != nil {
				return nil, nil // ambiguous: two platform labels on one job
			}
			chosen = p
		}
	}
	if chosen == nil {
		return nil, nil
	}
	var custom []string
	for _, l := range jobLabels {
		if !builtinLabels[strings.ToLower(l)] {
			custom = append(custom, l)
		}
	}
	for _, l := range c.cfg.Dispatch.ExtraLabels {
		if !lowerSet(custom)[strings.ToLower(l)] {
			custom = append(custom, l)
		}
	}
	if len(custom) == 0 {
		// Keep at least one custom label; the runner adds self-hosted and the
		// OS/arch labels of the machine it actually runs on.
		custom = []string{"oak"}
	}
	return chosen, custom
}

func (c *Controller) isSourceRepo(repo string) bool {
	return repo == "" || strings.EqualFold(repo, c.cfg.GitHub.SourceRepo)
}

// --- events from webhook / poller ---------------------------------------------------

func (c *Controller) JobQueued(job WorkflowJob, repo string) {
	if !c.isSourceRepo(repo) {
		return
	}
	platform, _ := c.platformFor(job.Labels)
	if platform == nil {
		return // not for us
	}
	c.mu.Lock()
	if _, ok := c.pending[job.ID]; !ok {
		if _, dispatched := c.legs[job.ID]; !dispatched {
			c.pending[job.ID] = &PendingJob{
				ID: job.ID, Name: job.Name, Labels: job.Labels,
				Platform: platform.Name, SeenAt: time.Now(),
			}
			c.log.Info("job queued", "job", job.ID, "name", job.Name,
				"platform", platform.Name, "labels", job.Labels)
			c.saveLocked()
		}
	}
	c.mu.Unlock()
	c.trigger()
}

func (c *Controller) JobInProgress(job WorkflowJob) {
	c.mu.Lock()
	delete(c.pending, job.ID)
	if leg := c.legs[job.ID]; leg != nil {
		c.log.Info("job started", "job", job.ID, "worker", leg.RunnerName, "platform", leg.Platform)
	}
	c.saveLocked()
	c.mu.Unlock()
}

func (c *Controller) JobCompleted(job WorkflowJob) {
	c.mu.Lock()
	delete(c.pending, job.ID)
	leg := c.legs[job.ID]
	delete(c.legs, job.ID)
	c.saveLocked()
	c.mu.Unlock()
	if leg != nil {
		c.log.Info("job completed", "job", job.ID, "conclusion", job.Conclusion, "worker", leg.RunnerName)
	}
	c.trigger()
}

// SyncQueued replaces the pending set with the authoritative queued list.
func (c *Controller) SyncQueued(jobs []WorkflowJob) {
	present := map[int64]bool{}
	for _, j := range jobs {
		present[j.ID] = true
	}
	c.mu.Lock()
	for id := range c.pending {
		if !present[id] {
			delete(c.pending, id)
		}
	}
	c.mu.Unlock()
	for _, j := range jobs {
		c.JobQueued(j, c.cfg.GitHub.SourceRepo)
	}
}

// --- reconcile ---------------------------------------------------------------------

// reconcile dispatches workers for queued jobs that no worker is handling yet.
func (c *Controller) reconcile() {
	if c.ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	if since := time.Since(c.lastFailure); since < c.cfg.Dispatch.FailureBackoff.Duration {
		c.mu.Unlock()
		return
	}
	free := c.cfg.Dispatch.MaxLegs - len(c.legs)
	var batch []*PendingJob
	for _, p := range c.pending {
		if free <= 0 {
			break
		}
		if _, dispatched := c.legs[p.ID]; dispatched {
			continue
		}
		batch = append(batch, p)
		free--
	}
	waiting := len(c.pending) - len(c.legs) - len(batch)
	c.mu.Unlock()

	if len(batch) == 0 {
		return
	}
	// Oldest first keeps GitHub's queue order roughly intact.
	sort.Slice(batch, func(i, j int) bool { return batch[i].SeenAt.Before(batch[j].SeenAt) })
	if waiting > 0 {
		c.log.Info("max_legs reached, jobs wait for the next round", "waiting", waiting)
	}
	if err := c.dispatch(batch); err != nil {
		c.log.Error("dispatch failed", "err", err)
		c.mu.Lock()
		c.lastFailure = time.Now()
		c.mu.Unlock()
	}
}

type workerSpec struct {
	OS   string `json:"os"`
	JIT  string `json:"jit"`
	Name string `json:"name"`
	V    string `json:"v"`
}

// dispatch creates one single-use runner per job, publishes them as a secret on
// the trigger repo, and commits a no-op change that starts the workflow.
func (c *Controller) dispatch(batch []*PendingJob) error {
	ctx, cancel := context.WithTimeout(c.ctx, 3*time.Minute)
	defer cancel()

	specs := make([]workerSpec, 0, len(batch))
	legs := make([]*Leg, 0, len(batch))
	for _, p := range batch {
		platform, labels := c.platformFor(p.Labels)
		if platform == nil {
			c.log.Warn("job no longer has a usable platform label", "job", p.ID)
			continue
		}
		name := fmt.Sprintf("%s%d-%s", c.cfg.Dispatch.NamePrefix, p.ID, randHex(2))
		jit, err := c.gh.GenerateJITConfig(ctx, c.cfg.GitHub.SourceRepo, name, labels)
		if err != nil {
			// Undo nothing: runners that were created but never used are
			// removed by the reaper.
			return fmt.Errorf("generate jit config for job %d: %w", p.ID, err)
		}
		specs = append(specs, workerSpec{
			OS: platform.RunnerOS, JIT: jit.EncodedJITConfig,
			Name: name, V: c.cfg.Dispatch.RunnerVersion,
		})
		legs = append(legs, &Leg{
			JobID: p.ID, JobName: p.Name, Platform: platform.Name,
			RunnerOS: platform.RunnerOS, RunnerName: name,
			RunnerID: jit.Runner.ID, Labels: labels, DispatchedAt: time.Now(),
		})
	}
	if len(specs) == 0 {
		return nil
	}

	payload, err := json.Marshal(specs)
	if err != nil {
		return err
	}
	if err := c.gh.SetRepoSecret(ctx, c.cfg.GitHub.TriggerRepo, c.cfg.Dispatch.SecretName, string(payload)); err != nil {
		return fmt.Errorf("write secret: %w", err)
	}
	// Workers send all egress through the proxy so third parties see this host.
	// The password is unique to this dispatch and expires with the workers, so
	// a leaked value is worthless after the batch is done.
	if c.proxy != nil {
		token := randHex(24)
		c.proxy.AddCredential(token, c.cfg.Dispatch.LegTTL.Duration+15*time.Minute)
		url := c.cfg.Proxy.WorkerURL(token)
		if url != "" {
			if err := c.gh.SetRepoSecret(ctx, c.cfg.GitHub.TriggerRepo, c.cfg.Proxy.SecretName, url); err != nil {
				return fmt.Errorf("write proxy secret: %w", err)
			}
		}
	}
	// Secrets are read by the workflow at run time; give the write a moment to
	// land before the commit makes it start.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.cfg.Dispatch.SecretSettle.Duration):
	}

	nonce := randHex(4)
	msg := strings.NewReplacer("{count}", fmt.Sprint(len(specs)), "{nonce}", nonce).
		Replace(c.cfg.Dispatch.CommitMessage)
	content := fmt.Sprintf("%s %s\n", nonce, time.Now().UTC().Format(time.RFC3339))
	if err := c.commit(ctx, content, msg); err != nil {
		return fmt.Errorf("commit trigger file: %w", err)
	}

	c.mu.Lock()
	for _, leg := range legs {
		delete(c.pending, leg.JobID)
		c.legs[leg.JobID] = leg
	}
	c.saveLocked()
	c.mu.Unlock()
	for _, leg := range legs {
		c.log.Info("worker dispatched", "job", leg.JobID, "platform", leg.Platform,
			"worker", leg.RunnerName, "runner_id", leg.RunnerID)
	}
	return nil
}

func (c *Controller) commit(ctx context.Context, content, message string) error {
	// Retry on conflict: the file may change between the read and the write.
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		sha, err := c.gh.FileSHA(ctx, c.cfg.GitHub.TriggerRepo, c.cfg.Dispatch.TriggerFile)
		if err != nil {
			return err
		}
		err = c.gh.CommitFile(ctx, c.cfg.GitHub.TriggerRepo, c.cfg.Dispatch.TriggerFile,
			content, sha, message, c.cfg.Dispatch.AuthorName, c.cfg.Dispatch.AuthorEmail)
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
	return lastErr
}

// --- reaper -------------------------------------------------------------------------

// reap forgets finished jobs, expires stale workers and removes runner
// registrations that no worker will ever use.
func (c *Controller) reap() {
	if c.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Minute)
	defer cancel()
	now := time.Now()

	c.mu.Lock()
	var check []*Leg
	var expired []*Leg
	for _, leg := range c.legs {
		age := now.Sub(leg.DispatchedAt)
		if age > c.cfg.Dispatch.LegTTL.Duration {
			expired = append(expired, leg)
			continue
		}
		// Runners need a moment to register before GitHub knows about the job.
		if age > time.Minute {
			check = append(check, leg)
		}
	}
	for id, p := range c.pending {
		if now.Sub(p.SeenAt) > c.cfg.Dispatch.PendingJobTTL.Duration {
			c.log.Warn("dropping queued job that was never served", "job", id, "name", p.Name)
			delete(c.pending, id)
		}
	}
	legNames := map[string]bool{}
	for _, leg := range c.legs {
		legNames[leg.RunnerName] = true
	}
	c.saveLocked()
	c.mu.Unlock()

	for _, leg := range expired {
		c.log.Warn("worker exceeded leg_ttl, releasing it", "job", leg.JobID,
			"worker", leg.RunnerName, "age", now.Sub(leg.DispatchedAt).Round(time.Second))
		c.finishLeg(ctx, leg)
	}

	for _, leg := range check {
		job, err := c.gh.Job(ctx, c.cfg.GitHub.SourceRepo, leg.JobID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				c.finishLeg(ctx, leg)
				continue
			}
			c.log.Warn("check job state", "job", leg.JobID, "err", err)
			continue
		}
		if job.Status == "completed" {
			c.log.Info("job finished, worker released", "job", leg.JobID, "conclusion", job.Conclusion)
			c.finishLeg(ctx, leg)
		}
	}

	// Registrations created by a failed dispatch never come online.
	runners, err := c.gh.ListRunners(ctx, c.cfg.GitHub.SourceRepo)
	if err != nil {
		c.log.Warn("list runners", "err", err)
		return
	}
	for _, r := range runners {
		if !strings.HasPrefix(r.Name, c.cfg.Dispatch.NamePrefix) || r.Status != "offline" || legNames[r.Name] {
			continue
		}
		c.mu.Lock()
		ageGone := true
		for _, leg := range c.legs {
			if leg.RunnerID == r.ID {
				ageGone = false
				break
			}
		}
		c.mu.Unlock()
		if !ageGone {
			continue
		}
		c.log.Info("removing unused runner registration", "runner", r.Name, "id", r.ID)
		if err := c.gh.DeleteRunner(ctx, c.cfg.GitHub.SourceRepo, r.ID); err != nil {
			c.log.Warn("delete runner", "runner", r.Name, "err", err)
		}
	}
}

// finishLeg stops tracking a worker and cleans up its registration.
func (c *Controller) finishLeg(ctx context.Context, leg *Leg) {
	c.mu.Lock()
	if c.legs[leg.JobID] != leg {
		c.mu.Unlock()
		return
	}
	delete(c.legs, leg.JobID)
	delete(c.pending, leg.JobID)
	c.saveLocked()
	c.mu.Unlock()

	// Ephemeral runners deregister themselves; this is a safety net.
	if err := c.gh.DeleteRunner(ctx, c.cfg.GitHub.SourceRepo, leg.RunnerID); err != nil {
		c.log.Warn("delete runner registration", "runner", leg.RunnerName, "err", err)
	}
	c.trigger()
}

// --- state ---------------------------------------------------------------------------

func (c *Controller) loadState() error {
	raw, err := os.ReadFile(c.cfg.StateFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var st persistedState
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("parse state file %s: %w", c.cfg.StateFile, err)
	}
	for _, p := range st.Pending {
		c.pending[p.ID] = p
	}
	for _, leg := range st.Legs {
		c.legs[leg.JobID] = leg
	}
	c.log.Info("state loaded", "pending", len(c.pending), "legs", len(c.legs))
	return nil
}

func (c *Controller) saveLocked() {
	raw, err := json.MarshalIndent(c.snapshotLocked(), "", "  ")
	if err != nil {
		c.log.Error("marshal state", "err", err)
		return
	}
	if dir := filepath.Dir(c.cfg.StateFile); dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := c.cfg.StateFile + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		c.log.Error("write state", "err", err)
		return
	}
	if err := os.Rename(tmp, c.cfg.StateFile); err != nil {
		c.log.Error("rename state", "err", err)
	}
}

func (c *Controller) snapshotLocked() persistedState {
	st := persistedState{Pending: []*PendingJob{}, Legs: []*Leg{}}
	for _, p := range c.pending {
		cp := *p
		st.Pending = append(st.Pending, &cp)
	}
	for _, l := range c.legs {
		cp := *l
		cp.Labels = slices.Clone(l.Labels)
		st.Legs = append(st.Legs, &cp)
	}
	sort.Slice(st.Pending, func(i, j int) bool { return st.Pending[i].ID < st.Pending[j].ID })
	sort.Slice(st.Legs, func(i, j int) bool { return st.Legs[i].DispatchedAt.Before(st.Legs[j].DispatchedAt) })
	return st
}

// Snapshot returns a copy of the current state for the status endpoint.
func (c *Controller) Snapshot() persistedState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked()
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
