# oak_runner

Use GitHub-hosted runners on a public repo to execute jobs that belong to a
private repo, while the private repo keeps the self-hosted runner.

```
push to soupskin-b2/oak (private, self-hosted runner = this controller on the VPS)
        |
        v
controller: mint a single-use JIT runner credential for EACH queued job
controller: write credentials to a secret on soupskin-b2/oak_runner (public)
controller: commit .trigger/dispatch  ->  starts the workflow there
        |
        v
GitHub-hosted Linux / Windows / macOS machines start (free minutes, public repo)
each one: ./run.sh --jitconfig <credential>  ->  registers back to soupskin-b2/oak
each one: runs exactly ONE oak job, then exits and is deregistered
        |
        v
controller sees the job complete, drops the worker record
```

Workers send all egress through the controller's built-in forward proxy, so
third-party services see the VPS address.

**What this does not hide.** GitHub hands the credential to its own machine and
the runner registers from that machine, so GitHub itself always knows which
hosted runner executed the job. The proxy changes what *other* services see, not
what GitHub sees.

**Policy.** GitHub's terms restrict Actions usage to work related to the
repository it runs in. Using a public repo purely as a compute pool for another
repo's jobs is outside that, and is a plausible path to a suspension. That is a
decision for whoever owns the account.

## Components

| Path | Purpose |
| --- | --- |
| `main.go`, `config.go` | startup, YAML config with `${ENV}` expansion |
| `controller.go` | reconcile loop, dispatch, reaping, state file |
| `github.go` | JIT credentials, secret writes, file commits, job polling |
| `proxy.go` | authenticated forward proxy for worker egress |
| `webhook.go`, `poller.go` | two ways to learn about queued jobs |
| `.github/workflows/oak-worker.yml` | turns the dispatch secret into workers |
| `.github/workflows/controller.yml` | builds this binary |
| `deploy/` | systemd unit for the controller host |

## Requirements

Controller host: Linux, outbound HTTPS, one free TCP port for the API and one
for the proxy. No root needed, the proxy removes the need for Squid.

Credentials for the controller, usable on **both** repos:

- GitHub App installed on both repos with:
  - `oak`: Administration read/write (runner credentials), Actions read (polling)
  - `oak_runner`: Secrets read/write, Contents read/write
- or a fine-grained PAT with those same permissions.

Repo secrets on `oak_runner` are written by the controller, do not create them
by hand: `OAK_DISPATCH` (worker credentials) and `OAK_PROXY_URL` (proxy URL).

## Source repo setup

The self-hosted runner for `oak` is the controller host. Register it once:

```sh
# on the controller host, as the service user
mkdir -p ~/oak-worker && cd ~/oak-worker
curl -fsSL -o r.tar.gz https://github.com/actions/runner/releases/download/v2.329.0/actions-runner-linux-x64-2.329.0.tar.gz
tar xzf r.tar.gz && ./bin/installdependencies.sh
./config.sh --url https://github.com/soupskin-b2/oak --token <runner-registration-token>
./run.sh            # or install it as a service
```

Job labels decide the platform. A job runs on a worker when it asks for
`self-hosted` plus a platform label (`linux`, `windows` or `macos`). Any other
labels on the job are added to the generated runner, so a job asking for
`runs-on: [self-hosted, linux, ubuntu-24]` gets a runner labelled `ubuntu-24`
on an `ubuntu-latest` machine.

Platform labels are cosmetic here: a `windows` job lands on a GitHub-hosted
Windows machine, so the label must match reality.

## Trigger repo setup

`.trigger/dispatch` must exist on `main`. Commits to it are the trigger; the
worker workflow only runs for paths under `.trigger/`.

Each dispatched worker consumes a hosted runner minute. Public repos are free,
but concurrent job limits apply (20 on the free plan).

## Run the controller

```sh
cp config.example.yaml config.yaml     # edit repos, proxy settings
printf 'GITHUB_TOKEN=github_pat_...\n' > env
chmod 600 env

GOOS=linux GOARCH=amd64 go build -o oak-runner-controller .
./oak-runner-controller -config config.yaml
```

As a user service (no root needed, lingering is already on for this host):

```sh
mkdir -p ~/.config/systemd/user
cp deploy/oak-runner-controller.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now oak-runner-controller
journalctl --user -u oak-runner-controller -f
```

Endpoints: `GET /healthz`, `GET /status` (pending jobs and live workers as JSON).

## Behaviour

- **Batching.** Queued jobs are collected per reconcile tick and dispatched with a
  single secret write and a single commit, so one commit starts exactly the
  workers that are needed.
- **Platform choice.** A job with a `linux`, `windows` or `macos` label maps to the
  matching hosted runner. A job with no platform label is ignored. A job with two
  platform labels is ignored.
- **Cleanup.** A worker is released when its job reports `completed`, when
  `leg_ttl` expires, or when its registration is found offline with no matching
  worker record. Nothing is left running on your side: the machine belongs to
  GitHub and ends with the job.
- **Failure.** A failed dispatch pauses dispatching for `failure_backoff`, and
  credentials created for jobs that were never served are deleted by the reaper.
- **Restarts.** `state_file` keeps pending jobs and live workers, so a restart
  does not double-dispatch.
- **Proxy.** Sends worker egress from this host. Controls, strongest first:
  - only sources inside GitHub's published Actions ranges are accepted
    (`allow_github_ips`, refreshed hourly from the meta API)
  - the password is generated per dispatch and expires with the workers, so it
    is short-lived even though it travels in clear text
  - `CONNECT` only to 443/8443/563, plain HTTP only to 80/443
  - destinations resolving to private, loopback, link-local, CGNAT or metadata
    addresses are refused
  - 128 concurrent connections maximum

  Left open: anyone inside GitHub's ranges who also learns the password could
  relay through the host for the life of one batch. Set `proxy.pass` empty and
  rotate nothing else; there is no long-term credential to leak.

## Limits

- A hosted job is killed at 6 hours, so no `oak` job can exceed that.
- `max_legs` bounds how many workers run at once. Jobs beyond it stay queued on
  `oak` until the next reconcile.
- macOS workers are arm64 only (`macos-latest`). A job asking for `arm64` or
  `macos` works; one asking for an Intel macOS runner does not.
- Windows workers are Windows Server 2025 images; jobs needing desktop features
  or interactive sessions will not work.
