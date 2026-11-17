// Command oak-runner-controller runs on a host that can reach GitHub. When a job
// is queued on the source repo it mints single-use runner credentials, hands
// them to the public trigger repo as a secret, and pushes a throwaway commit
// that starts GitHub-hosted Linux, Windows and macOS workers. Each worker
// registers back to the source repo, runs exactly one job and disappears.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	debug := flag.Bool("debug", false, "debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}
	gh, err := NewGitHub(cfg.GitHub)
	if err != nil {
		log.Error("github client", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctrl, err := NewController(ctx, cfg, gh, log)
	if err != nil {
		log.Error("init controller", "err", err)
		os.Exit(1)
	}

	done := make(chan struct{})
	go func() { ctrl.Run(); close(done) }()
	if cfg.Poll.Enabled {
		go runPoller(ctx, ctrl, gh, cfg, log)
	}
	if cfg.Proxy.Enabled {
		p := NewProxy(cfg.Proxy, log)
		go func() {
			if err := p.Run(ctx); err != nil {
				log.Error("proxy stopped", "err", err)
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(ctrl.Snapshot())
	})
	if cfg.Webhook.Enabled {
		mux.Handle(cfg.Webhook.Path, webhookHandler(ctrl, cfg, log))
	}
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("listening", "addr", cfg.Listen, "source", cfg.GitHub.SourceRepo,
			"trigger", cfg.GitHub.TriggerRepo, "webhook", cfg.Webhook.Enabled, "poll", cfg.Poll.Enabled)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Info("signal received, stopping")

	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = srv.Shutdown(shCtx)
	shCancel()
	cancel()
	<-done
	log.Info("stopped")
}
