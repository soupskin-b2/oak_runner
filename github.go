package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/nacl/box"
)

// ErrNotFound is returned for 404 responses.
var ErrNotFound = errors.New("not found")

type GitHub struct {
	cfg  GitHubConfig
	http *http.Client

	appKey *rsa.PrivateKey
	mu     sync.Mutex
	token  string
	expiry time.Time
}

func NewGitHub(cfg GitHubConfig) (*GitHub, error) {
	g := &GitHub{cfg: cfg, http: &http.Client{Timeout: 30 * time.Second}}
	if cfg.App.AppID != "" {
		pemBytes, err := os.ReadFile(cfg.App.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("read app private key: %w", err)
		}
		block, _ := pem.Decode(pemBytes)
		if block == nil {
			return nil, errors.New("app private key: no PEM block found")
		}
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			k8, err8 := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err8 != nil {
				return nil, fmt.Errorf("parse app private key: %w", err)
			}
			var ok bool
			if key, ok = k8.(*rsa.PrivateKey); !ok {
				return nil, errors.New("app private key is not RSA")
			}
		}
		g.appKey = key
	}
	return g, nil
}

// --- auth -------------------------------------------------------------------

func (g *GitHub) authToken(ctx context.Context) (string, error) {
	if g.appKey == nil {
		return g.cfg.Token, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.token != "" && time.Until(g.expiry) > 5*time.Minute {
		return g.token, nil
	}
	jwt, err := g.appJWT()
	if err != nil {
		return "", err
	}
	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	path := fmt.Sprintf("/app/installations/%d/access_tokens", g.cfg.App.InstallationID)
	if err := g.doWithAuth(ctx, "Bearer "+jwt, http.MethodPost, path, nil, &out); err != nil {
		return "", fmt.Errorf("create installation token: %w", err)
	}
	g.token, g.expiry = out.Token, out.ExpiresAt
	return g.token, nil
}

func (g *GitHub) appJWT() (string, error) {
	now := time.Now()
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{
		"iat": now.Add(-60 * time.Second).Unix(),
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": g.cfg.App.AppID,
	})
	signing := header + "." + enc.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, g.appKey, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return signing + "." + enc.EncodeToString(sig), nil
}

// --- transport ----------------------------------------------------------------

func (g *GitHub) do(ctx context.Context, method, path string, body, out any) error {
	tok, err := g.authToken(ctx)
	if err != nil {
		return err
	}
	return g.doWithAuth(ctx, "Bearer "+tok, method, path, body, out)
}

func (g *GitHub) doWithAuth(ctx context.Context, auth, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, g.cfg.APIURL+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "oak-runner-controller")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%s %s: %w", method, path, ErrNotFound)
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, truncate(string(data), 500))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

// --- runners (source repo) ------------------------------------------------------

type Runner struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"` // online / offline
	Busy   bool   `json:"busy"`
}

type JITConfig struct {
	Runner           Runner `json:"runner"`
	EncodedJITConfig string `json:"encoded_jit_config"`
}

func runnersPath(repo string) string {
	return "/repos/" + repo + "/actions/runners"
}

// GenerateJITConfig creates a single-use runner for repo.
func (g *GitHub) GenerateJITConfig(ctx context.Context, repo, name string, labels []string) (*JITConfig, error) {
	body := map[string]any{
		"name":            name,
		"runner_group_id": g.cfg.RunnerGroupID,
		"labels":          labels,
		"work_folder":     "_work",
	}
	out := &JITConfig{}
	if err := g.do(ctx, http.MethodPost, runnersPath(repo)+"/generate-jitconfig", body, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (g *GitHub) DeleteRunner(ctx context.Context, repo string, id int64) error {
	err := g.do(ctx, http.MethodDelete, runnersPath(repo)+"/"+strconv.FormatInt(id, 10), nil, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (g *GitHub) ListRunners(ctx context.Context, repo string) ([]Runner, error) {
	var all []Runner
	for page := 1; ; page++ {
		var out struct {
			TotalCount int      `json:"total_count"`
			Runners    []Runner `json:"runners"`
		}
		path := fmt.Sprintf("%s?per_page=100&page=%d", runnersPath(repo), page)
		if err := g.do(ctx, http.MethodGet, path, nil, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Runners...)
		if len(out.Runners) < 100 || len(all) >= out.TotalCount {
			return all, nil
		}
	}
}

// --- jobs (source repo) ---------------------------------------------------------

type WorkflowJob struct {
	ID         int64    `json:"id"`
	RunID      int64    `json:"run_id"`
	Name       string   `json:"name"`
	Status     string   `json:"status"`
	Conclusion string   `json:"conclusion"`
	Labels     []string `json:"labels"`
	RunnerID   int64    `json:"runner_id"`
	RunnerName string   `json:"runner_name"`
}

func (g *GitHub) Job(ctx context.Context, repo string, id int64) (*WorkflowJob, error) {
	out := &WorkflowJob{}
	err := g.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/actions/jobs/%d", repo, id), nil, out)
	return out, err
}

// QueuedJobs returns all queued jobs of queued or in-progress runs.
func (g *GitHub) QueuedJobs(ctx context.Context, repo string) ([]WorkflowJob, error) {
	var jobs []WorkflowJob
	for _, status := range []string{"queued", "in_progress"} {
		var runs struct {
			WorkflowRuns []struct {
				ID int64 `json:"id"`
			} `json:"workflow_runs"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs?status=%s&per_page=100", repo, status)
		if err := g.do(ctx, http.MethodGet, path, nil, &runs); err != nil {
			return nil, err
		}
		for _, r := range runs.WorkflowRuns {
			var out struct {
				Jobs []WorkflowJob `json:"jobs"`
			}
			path := fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?filter=latest&per_page=100", repo, r.ID)
			if err := g.do(ctx, http.MethodGet, path, nil, &out); err != nil {
				return nil, err
			}
			for _, j := range out.Jobs {
				if j.Status == "queued" {
					jobs = append(jobs, j)
				}
			}
		}
	}
	return jobs, nil
}

// --- dispatch repo: secrets and contents --------------------------------------------

// SetRepoSecret writes an Actions secret on repo. Values are encrypted with the
// repo's public key as GitHub requires.
func (g *GitHub) SetRepoSecret(ctx context.Context, repo, name, value string) error {
	var key struct {
		KeyID string `json:"key_id"`
		Key   string `json:"key"`
	}
	if err := g.do(ctx, http.MethodGet, "/repos/"+repo+"/actions/secrets/public-key", nil, &key); err != nil {
		return err
	}
	rawKey, err := base64.StdEncoding.DecodeString(key.Key)
	if err != nil {
		return fmt.Errorf("decode public key: %w", err)
	}
	if len(rawKey) != 32 {
		return fmt.Errorf("public key has length %d, want 32", len(rawKey))
	}
	var pk [32]byte
	copy(pk[:], rawKey)
	sealed, err := box.SealAnonymous(nil, []byte(value), &pk, rand.Reader)
	if err != nil {
		return fmt.Errorf("seal secret: %w", err)
	}
	body := map[string]string{
		"encrypted_value": base64.StdEncoding.EncodeToString(sealed),
		"key_id":          key.KeyID,
	}
	return g.do(ctx, http.MethodPut, "/repos/"+repo+"/actions/secrets/"+name, body, nil)
}

// FileSHA returns the blob SHA of a file, or "" when it does not exist.
func (g *GitHub) FileSHA(ctx context.Context, repo, path string) (string, error) {
	var out struct {
		SHA string `json:"sha"`
	}
	err := g.do(ctx, http.MethodGet, "/repos/"+repo+"/contents/"+path, nil, &out)
	if errors.Is(err, ErrNotFound) {
		return "", nil
	}
	return out.SHA, err
}

// CommitFile creates or updates one file on repo's default branch. An empty sha
// creates the file.
func (g *GitHub) CommitFile(ctx context.Context, repo, path, content, sha, message, authorName, authorEmail string) error {
	body := map[string]any{
		"message": message,
		"content": base64.StdEncoding.EncodeToString([]byte(content)),
		"author": map[string]string{
			"name":  authorName,
			"email": authorEmail,
		},
		"committer": map[string]string{
			"name":  authorName,
			"email": authorEmail,
		},
	}
	if sha != "" {
		body["sha"] = sha
	}
	return g.do(ctx, http.MethodPut, "/repos/"+repo+"/contents/"+path, body, nil)
}

// LatestRunnerVersion returns the current actions/runner release, without the
// leading "v".
func (g *GitHub) LatestRunnerVersion(ctx context.Context) (string, error) {
	var out struct {
		TagName string `json:"tag_name"`
	}
	if err := g.do(ctx, http.MethodGet, "/repos/actions/runner/releases/latest", nil, &out); err != nil {
		return "", err
	}
	v := strings.TrimPrefix(out.TagName, "v")
	if v == "" {
		return "", errors.New("empty runner version in release response")
	}
	return v, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
