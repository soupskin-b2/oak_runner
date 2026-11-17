package main

import (
	"context"
	"log/slog"
	"time"
)

// poller lists queued jobs on the source repo via the REST API. It needs no
// inbound connectivity and also repairs state after missed webhooks.
func runPoller(ctx context.Context, c *Controller, gh *GitHub, cfg *Config, log *slog.Logger) {
	t := time.NewTicker(cfg.Poll.Interval.Duration)
	defer t.Stop()
	for {
		jobs, err := gh.QueuedJobs(ctx, cfg.GitHub.SourceRepo)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("poll queued jobs", "err", err)
			}
		} else {
			c.SyncQueued(jobs)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
