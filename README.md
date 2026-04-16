# Schuldockbot

Schuldockbot is a Python runtime that polls official Schuldock notice feeds and posts NEW/UPDATE events into a configured Nextcloud Talk room.

## Repository and Distribution Coordinates

- Canonical repository target: **Borega/Schuldockbot**
- Planned container image: **ghcr.io/borega/schuldockbot**

## Current Baseline and Next Slices

This slice establishes repository policy + bootstrap documentation.

- **S02 (next)** will add runnable `docker` image + `compose` example for long-running service execution.
- **S03 (next)** will add `GitHub Actions` workflow automation to build and publish `ghcr.io/borega/schuldockbot`.

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

## Runtime Verification Commands

Use these commands from repository root to validate startup wiring before deployment:

```bash
python -m schuldockbot.runtime.main --once --dry-run
python scripts/verify_runtime_dry_run.py
```

## Runtime Operations

For Linux ARM systemd deployment/operations details, see:

- `docs/operations/systemd.md`

## Policy Notice (Non-Commercial + Attribution)

This project is provided for **non-commercial** use only. Any reuse, redistribution, or derivative work must include clear **attribution** and a reference to the original source (OG): `Borega/Schuldockbot`.

Commercial use is not allowed without prior **written permission** from the rights holder. See `LICENSE` for the binding policy text.
