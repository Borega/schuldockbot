# Schuldockbot

Schuldockbot is a Python runtime that polls official Schuldock notice feeds and posts NEW/UPDATE events into a configured Nextcloud Talk room.

## Repository and Distribution Coordinates

- Canonical repository target: **Borega/Schuldockbot**
- Planned container image: **ghcr.io/borega/schuldockbot**

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

## Runtime Verification (host Python)

Use these commands from repository root to validate startup wiring before deployment:

```bash
python -m schuldockbot.runtime.main --once --dry-run
python scripts/verify_runtime_dry_run.py
```

## Container + Compose Preflight (shell-agnostic)

1. Copy `.env.example` to a local env file (for example `.env.local`) and replace placeholders with real values.
2. Keep secrets out of git; do not commit room tokens/app passwords.
3. Run one deterministic verification command that validates image build + dry-run startup + compose contract:

```bash
python scripts/verify_container_compose.py
```

Equivalent manual steps (same checks, explicit commands):

```bash
docker build -t schuldockbot:s02 .
docker run --rm --env-file .env.example schuldockbot:s02 --once --dry-run
docker compose --env-file .env.example config
docker compose --env-file .env.example run --rm schuldockbot --once --dry-run
```

## Long-running Compose Operator Evidence

For real service operation and observability evidence, first create `.env` from `.env.example` (with non-secret placeholders replaced locally), then run:

```bash
docker compose up -d
docker compose logs --tail=100 schuldockbot
docker compose ps
CONTAINER_ID=$(docker compose ps -q schuldockbot)
docker inspect --format '{{json .State.Health}}' "$CONTAINER_ID"
```

Expected runtime JSON events in logs include `startup`, `cycle_started`, `cycle_completed`, and `shutdown`.

## Runtime Operations

- Container/compose operations are above.
- Linux ARM systemd host deployment/operations details: `docs/operations/systemd.md`

## Policy Notice (Non-Commercial + Attribution)

This project is provided for **non-commercial** use only. Any reuse, redistribution, or derivative work must include clear **attribution** and a reference to the original source (OG): `Borega/Schuldockbot`.

Commercial use is not allowed without prior **written permission** from the rights holder. See `LICENSE` for the binding policy text.
