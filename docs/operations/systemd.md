# Schuldockbot systemd operations (ARM Linux)

This runbook installs and operates `schuldockbot` as an unattended systemd service with structured journald output.

## 1) Host prerequisites

- ARM Linux host with `systemd` (`systemctl` + `journalctl` available)
- Python 3.11+
- Repository checked out at `/opt/schuldockbot`
- Runtime user/group `schuldockbot`

```bash
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin schuldockbot 2>/dev/null || true
sudo install -d -o schuldockbot -g schuldockbot -m 0750 /var/lib/schuldockbot
sudo install -d -o root -g root -m 0755 /etc/schuldockbot
```

## 2) Environment file

Create `/etc/schuldockbot/schuldockbot.env` (0600) with **all required keys**.

```bash
sudo install -m 0600 /dev/null /etc/schuldockbot/schuldockbot.env
sudoedit /etc/schuldockbot/schuldockbot.env
```

Required keys:

- `SCHULDOCKBOT_SOURCE_JSON_URL`
- `SCHULDOCKBOT_SOURCE_HTML_URL`
- `SCHULDOCKBOT_STATE_DB_PATH`
- `SCHULDOCKBOT_TALK_BASE_URL`
- `SCHULDOCKBOT_TALK_ROOM_TOKEN`
- `SCHULDOCKBOT_TALK_USERNAME`
- `SCHULDOCKBOT_TALK_APP_PASSWORD`

Optional keys (runtime defaults apply if omitted):

- `SCHULDOCKBOT_POLL_INTERVAL_SECONDS` (default `600`, min `10`, max `86400`)
- `SCHULDOCKBOT_RETRY_MAX_ATTEMPTS` (default `3`, min `1`, max `20`)
- `SCHULDOCKBOT_RETRY_BACKOFF_SECONDS` (default `30.0`, min `1`, max `3600`)
- `SCHULDOCKBOT_SQLITE_TIMEOUT_SECONDS` (default `5.0`, min `0.1`, max `120`)
- `SCHULDOCKBOT_TALK_TIMEOUT_SECONDS` (default `10.0`, min `0.1`, max `120`)

Safe sample (replace placeholders with real values; do **not** commit secrets):

```dotenv
SCHULDOCKBOT_SOURCE_JSON_URL=https://schuldock.hamburg/schuldock/v1/issues
SCHULDOCKBOT_SOURCE_HTML_URL=https://schuldock.hamburg/aktuelle-meldungen
SCHULDOCKBOT_STATE_DB_PATH=/var/lib/schuldockbot/state.db
SCHULDOCKBOT_TALK_BASE_URL=https://cloud.example
SCHULDOCKBOT_TALK_ROOM_TOKEN=replace-me
SCHULDOCKBOT_TALK_USERNAME=schuldockbot
SCHULDOCKBOT_TALK_APP_PASSWORD=replace-me
SCHULDOCKBOT_POLL_INTERVAL_SECONDS=600
SCHULDOCKBOT_RETRY_MAX_ATTEMPTS=3
SCHULDOCKBOT_RETRY_BACKOFF_SECONDS=30
```

If required keys are missing/invalid, runtime exits fast with `startup_validation_failed` in journald.

## 3) Install and enable the unit

```bash
sudo install -D -m 0644 deploy/systemd/schuldockbot.service /etc/systemd/system/schuldockbot.service
sudo systemd-analyze verify /etc/systemd/system/schuldockbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now schuldockbot.service
```

## 4) Operator lifecycle commands

```bash
sudo systemctl status schuldockbot.service --no-pager
sudo systemctl restart schuldockbot.service
sudo systemctl stop schuldockbot.service
sudo systemctl start schuldockbot.service
sudo systemctl disable schuldockbot.service
```

Structured logs (JSON-compatible output for diagnostics):

```bash
sudo journalctl -u schuldockbot.service -n 100 -o json
sudo journalctl -u schuldockbot.service -f -o cat
```

## 5) Troubleshooting flow

### Service failed to start

1. Check status for exit code and restart state:
   ```bash
   sudo systemctl status schuldockbot.service --no-pager
   ```
2. Inspect recent startup events:
   ```bash
   sudo journalctl -u schuldockbot.service -n 50 -o json
   ```
3. Look for `startup_validation_failed` and fix missing/invalid env keys in `/etc/schuldockbot/schuldockbot.env`.

### Repeated restart churn (10x failure loop risk)

1. Confirm restart count:
   ```bash
   sudo systemctl show schuldockbot.service -p NRestarts --no-pager
   ```
2. Inspect latest fatal cycle details:
   ```bash
   sudo journalctl -u schuldockbot.service -n 100 -o json
   ```
3. Resolve root cause from `phase`, `failure_class`, `retryable`, `retry_attempt`, and `sleep_seconds` fields.

### Slow/uncertain startup

1. Wait for service state transition:
   ```bash
   sudo systemctl status schuldockbot.service --no-pager
   ```
2. Verify recent journal events are moving (`startup`, then cycle events):
   ```bash
   sudo journalctl -u schuldockbot.service -n 30 -o json
   ```

## 6) Boundary condition: custom cadence without code edits

Adjust polling/retry behavior by editing only env values and restarting:

```bash
sudoedit /etc/schuldockbot/schuldockbot.env
sudo systemctl restart schuldockbot.service
```

## 7) Repository-side preflight gates (run before host evidence)

Run these commands from repo root. Do **not** start S05 host evidence collection until all pass.

```bash
python -m pytest tests/integration/test_runtime_end_to_end.py -q
python -m pytest tests/unit/ingestion tests/unit/state tests/unit/talk tests/unit/runtime tests/integration/test_runtime_end_to_end.py -q
python scripts/verify_runtime_dry_run.py
python -m pytest tests/unit/runtime/test_service_unit_contract.py -q
python -c "import shutil, subprocess, sys; exe = shutil.which('systemd-analyze'); sys.exit(0 if exe is None else subprocess.call([exe, 'verify', 'deploy/systemd/schuldockbot.service']))"
```

If any preflight command fails, treat host proof as **blocked** and fix repo/runtime regressions first.

## 8) S05 live smoke + 24h stability evidence

This section is the milestone-close host evidence workflow for R009.

### 8.1 Prerequisites

- `schuldockbot.service` is installed from `deploy/systemd/schuldockbot.service` and enabled.
- `/etc/schuldockbot/schuldockbot.env` has real credentials (not placeholders).
- Section 7 preflight commands all passed in the same revision you deployed.

### 8.2 Live smoke capture (single operator run)

Run and archive output in order:

```bash
sudo systemctl status schuldockbot.service --no-pager
sudo journalctl -u schuldockbot.service -n 200 -o json
```

Required live-smoke evidence fields from journal JSON:

- Lifecycle continuity: `event` includes `startup`, `cycle_started`, `cycle_completed`, and `shutdown` (or ongoing service with repeated `cycle_started`/`cycle_completed` if no shutdown yet).
- Cycle diagnostics: `phase` and `failure_class` present for failure/retry events.
- Delivery counters on cycle payloads: `attempted`, `posted`, `failed`, `acked`.
- Source behavior fields where available: `source_mode`, `fallback_reason`.

Live smoke pass/fail interpretation:

- **Pass**: service is active/running, events are well-formed JSON, and cycle counters are coherent (`attempted >= posted`, `attempted >= failed`, `posted >= acked`).
- **Fail**: service in failed/restarting state, missing structured event keys, or counters/events absent so behavior is not verifiable.

### 8.3 24h stability capture

Collect continuity + restart evidence:

```bash
sudo journalctl -u schuldockbot.service --since "24 hours ago" -o json
sudo systemctl show schuldockbot.service -p NRestarts --no-pager
```

Required 24h evidence fields:

- Event continuity across the full window (`cycle_started`/`cycle_completed` progression).
- Any fault events include `phase`, `failure_class`, `retryable`, and retry metadata.
- Restart churn via `NRestarts`.

24h boundary condition guidance:

- A window with **no new notices** can still pass if heartbeat/cycle events are continuous and failure diagnostics show no unresolved fatal churn.
- Elevated retries or growing `NRestarts` require triage before milestone close.

### 8.4 Failure-mode mapping and next actions

- **`systemctl` errors or unit mismatch**: confirm exact unit name and loaded file.
  ```bash
  sudo systemctl status schuldockbot.service --no-pager
  sudo systemctl cat schuldockbot.service
  ```
  If directives differ from repo contract, reinstall unit, run `daemon-reload`, restart, then recapture status+journal evidence.
- **Timeout / unclear startup**: restart and require follow-up evidence (status + recent JSON journal) before declaring pass.
  ```bash
  sudo systemctl restart schuldockbot.service
  sudo systemctl status schuldockbot.service --no-pager
  sudo journalctl -u schuldockbot.service -n 100 -o json
  ```
- **Malformed / non-JSON journal output**: recapture with `-o json`; reject unverifiable captures that do not expose `event`, `phase`, and `failure_class` keys when failures occur.
- **Missing env keys / bad credentials**: expect `startup_validation_failed`; fix env file and restart, then capture fresh evidence.

### 8.5 Evidence hygiene and redaction rules

- Never copy raw secrets (`SCHULDOCKBOT_TALK_ROOM_TOKEN`, `SCHULDOCKBOT_TALK_APP_PASSWORD`, auth headers) into UAT notes, screenshots, or tickets.
- Keep captured evidence to service status, event names, diagnostic fields, counters, and restart metrics.
- If logs accidentally include sensitive material, redact before sharing and regenerate sanitized evidence when possible.
