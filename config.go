package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so it can be written as "30s", "5m" in YAML.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", n.Value, err)
	}
	d.Duration = v
	return nil
}

type Config struct {
	Listen    string         `yaml:"listen"`
	StateFile string         `yaml:"state_file"`
	Webhook   WebhookConfig  `yaml:"webhook"`
	Poll      PollConfig     `yaml:"poll"`
	GitHub    GitHubConfig   `yaml:"github"`
	Dispatch  DispatchConfig `yaml:"dispatch"`
	Proxy     ProxyConfig    `yaml:"proxy"`
}

// ProxyConfig controls the forward proxy workers use for egress.
type ProxyConfig struct {
	Enabled bool `yaml:"enabled"`
	// Addr is the listen address, e.g. ":3128".
	Addr string `yaml:"addr"`
	// PublicURL is how workers reach the proxy, e.g. http://203.0.113.5:3128.
	PublicURL string `yaml:"public_url"`
	// User and Pass authenticate workers. Never run the proxy without them.
	User string `yaml:"user"`
	Pass string `yaml:"pass"`
	// SecretName is the trigger repo secret holding the proxy URL.
	SecretName string `yaml:"secret_name"`
}

type WebhookConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
	Secret  string `yaml:"secret"`
}

type PollConfig struct {
	Enabled  bool     `yaml:"enabled"`
	Interval Duration `yaml:"interval"`
	// How often to check whether dispatched jobs finished, used for cleanup.
	JobCheckInterval Duration `yaml:"job_check_interval"`
}

type GitHubConfig struct {
	APIURL string `yaml:"api_url"`
	// SourceRepo is the private repo whose queued jobs get executed
	// ("owner/name"). Credentials are generated for this repo's runners.
	SourceRepo string `yaml:"source_repo"`
	// TriggerRepo is the public repo whose Actions run the workers
	// ("owner/name"). The controller writes secrets and commits to it.
	TriggerRepo   string `yaml:"trigger_repo"`
	RunnerGroupID int64  `yaml:"runner_group_id"`

	// Auth: either Token (PAT) or App.
	Token string          `yaml:"token"`
	App   GitHubAppConfig `yaml:"app"`
}

type GitHubAppConfig struct {
	AppID          string `yaml:"app_id"`
	InstallationID int64  `yaml:"installation_id"`
	PrivateKeyPath string `yaml:"private_key_path"`
}

type PlatformConfig struct {
	// Name is what the controller calls it: linux, windows, macos.
	Name string `yaml:"name"`
	// Label is the job label that selects this platform.
	Label string `yaml:"label"`
	// RunnerOS is the GitHub-hosted runner label used in the trigger workflow.
	RunnerOS string `yaml:"runner_os"`
}

type DispatchConfig struct {
	// SecretName holds the JSON batch of credentials in TriggerRepo.
	SecretName string `yaml:"secret_name"`
	// TriggerFile is committed to TriggerRepo; the change starts the workflow.
	TriggerFile   string `yaml:"trigger_file"`
	CommitMessage string `yaml:"commit_message"` // {count} and {nonce} are substituted
	AuthorName    string `yaml:"author_name"`
	AuthorEmail   string `yaml:"author_email"`

	// SecretSettle is how long to wait after writing the secret before
	// committing, so the workflow reads the new value.
	SecretSettle Duration `yaml:"secret_settle"`
	// RunnerVersion is the actions/runner version the worker downloads.
	RunnerVersion string `yaml:"runner_version"`

	NamePrefix string `yaml:"name_prefix"`
	// ExtraLabels are added to every generated runner.
	ExtraLabels []string `yaml:"extra_labels"`
	// MaxLegs caps how many workers may be in flight at once.
	MaxLegs int `yaml:"max_legs"`
	// LegTTL is how long to keep tracking a worker before giving up on it.
	LegTTL Duration `yaml:"leg_ttl"`
	// PendingJobTTL drops queued jobs the controller could not serve.
	PendingJobTTL Duration `yaml:"pending_job_ttl"`
	// ReconcileInterval is how often queued jobs are re-evaluated.
	ReconcileInterval Duration `yaml:"reconcile_interval"`
	// FailureBackoff pauses dispatching after a failure.
	FailureBackoff Duration `yaml:"failure_backoff"`

	Platforms []PlatformConfig `yaml:"platforms"`
}

func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Allow ${ENV_VAR} references so secrets stay out of the file.
	raw = []byte(os.ExpandEnv(string(raw)))

	c := &Config{}
	if err := yaml.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	return c, c.validate()
}

func (c *Config) applyDefaults() {
	def := func(d *Duration, v time.Duration) {
		if d.Duration == 0 {
			d.Duration = v
		}
	}
	if c.Listen == "" {
		c.Listen = ":8090"
	}
	if c.StateFile == "" {
		c.StateFile = "state.json"
	}
	if c.Webhook.Path == "" {
		c.Webhook.Path = "/webhook"
	}
	def(&c.Poll.Interval, 15*time.Second)
	def(&c.Poll.JobCheckInterval, 30*time.Second)
	if c.GitHub.APIURL == "" {
		c.GitHub.APIURL = "https://api.github.com"
	}
	c.GitHub.APIURL = strings.TrimRight(c.GitHub.APIURL, "/")
	if c.GitHub.RunnerGroupID == 0 {
		c.GitHub.RunnerGroupID = 1
	}
	d := &c.Dispatch
	if d.SecretName == "" {
		d.SecretName = "OAK_DISPATCH"
	}
	if d.TriggerFile == "" {
		d.TriggerFile = ".trigger/dispatch"
	}
	if d.CommitMessage == "" {
		d.CommitMessage = "ci: start {count} worker(s) [{nonce}]"
	}
	if d.AuthorName == "" {
		d.AuthorName = "oak-runner-controller"
	}
	if d.AuthorEmail == "" {
		d.AuthorEmail = "oak-runner-controller@users.noreply.github.com"
	}
	def(&d.SecretSettle, 5*time.Second)
	if d.RunnerVersion == "" {
		d.RunnerVersion = "2.329.0"
	}
	if d.NamePrefix == "" {
		d.NamePrefix = "oak-"
	}
	if d.MaxLegs == 0 {
		d.MaxLegs = 4
	}
	def(&d.LegTTL, 6*time.Hour)
	def(&d.PendingJobTTL, 10*time.Minute)
	def(&d.ReconcileInterval, 15*time.Second)
	def(&d.FailureBackoff, 1*time.Minute)
	if c.Proxy.Addr == "" {
		c.Proxy.Addr = ":3128"
	}
	if c.Proxy.SecretName == "" {
		c.Proxy.SecretName = "OAK_PROXY_URL"
	}
	if len(d.Platforms) == 0 {
		d.Platforms = []PlatformConfig{
			{Name: "linux", Label: "linux", RunnerOS: "ubuntu-latest"},
			{Name: "windows", Label: "windows", RunnerOS: "windows-latest"},
			{Name: "macos", Label: "macos", RunnerOS: "macos-latest"},
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	req := func(cond bool, msg string) {
		if !cond {
			errs = append(errs, errors.New(msg))
		}
	}
	req(c.Webhook.Enabled || c.Poll.Enabled, "enable webhook and/or poll")
	req(!c.Webhook.Enabled || c.Webhook.Secret != "", "webhook.secret is required when webhook is enabled")
	req(c.GitHub.SourceRepo != "" && strings.Contains(c.GitHub.SourceRepo, "/"), "github.source_repo must be owner/name")
	req(c.GitHub.TriggerRepo != "" && strings.Contains(c.GitHub.TriggerRepo, "/"), "github.trigger_repo must be owner/name")
	req(c.GitHub.SourceRepo != c.GitHub.TriggerRepo, "source_repo and trigger_repo must differ")
	hasApp := c.GitHub.App.AppID != ""
	req(c.GitHub.Token != "" || hasApp, "set github.token or github.app")
	if hasApp {
		req(c.GitHub.App.InstallationID != 0 && c.GitHub.App.PrivateKeyPath != "", "github.app needs installation_id and private_key_path")
	}
	seenLabel := map[string]bool{}
	for _, p := range c.Dispatch.Platforms {
		req(p.Name != "" && p.Label != "" && p.RunnerOS != "", "each platform needs name, label and runner_os")
		req(!seenLabel[strings.ToLower(p.Label)], "duplicate platform label "+p.Label)
		seenLabel[strings.ToLower(p.Label)] = true
	}
	req(len(c.Dispatch.Platforms) > 0, "at least one platform is required")
	if c.Proxy.Enabled {
		req(c.Proxy.User != "" && c.Proxy.Pass != "", "proxy.user and proxy.pass are required when the proxy is enabled")
		req(c.Proxy.PublicURL != "", "proxy.public_url is required when the proxy is enabled")
	}
	return errors.Join(errs...)
}
