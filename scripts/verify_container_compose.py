from __future__ import annotations

import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

REQUIRED_ENV_KEYS: tuple[str, ...] = (
    "SCHULDOCKBOT_SOURCE_JSON_URL",
    "SCHULDOCKBOT_SOURCE_HTML_URL",
    "SCHULDOCKBOT_STATE_DB_PATH",
    "SCHULDOCKBOT_TALK_BASE_URL",
    "SCHULDOCKBOT_TALK_ROOM_TOKEN",
    "SCHULDOCKBOT_TALK_USERNAME",
    "SCHULDOCKBOT_TALK_APP_PASSWORD",
)


@dataclass(frozen=True)
class Stage:
    label: str
    command: tuple[str, ...]
    timeout_seconds: int


def _format_command(command: tuple[str, ...]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def _print_stage_event(*, stage: str, status: str, detail: str | None = None) -> None:
    message = f"[verify-container-compose] stage={stage} status={status}"
    if detail:
        message = f"{message} detail={detail}"
    print(message)


def _collect_env_keys(env_file: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def _validate_env_contract(repo_root: Path) -> int:
    env_file = repo_root / ".env.example"
    stage = "env_contract"

    if not env_file.exists():
        _print_stage_event(stage=stage, status="failed", detail="missing .env.example")
        return 1

    env_values = _collect_env_keys(env_file)
    missing = [key for key in REQUIRED_ENV_KEYS if key not in env_values]
    blank = [key for key in REQUIRED_ENV_KEYS if not env_values.get(key, "").strip()]

    if missing or blank:
        details: list[str] = []
        if missing:
            details.append(f"missing={','.join(missing)}")
        if blank:
            details.append(f"blank={','.join(blank)}")

        _print_stage_event(stage=stage, status="failed", detail=";".join(details))
        return 1

    _print_stage_event(stage=stage, status="passed", detail=f"file={env_file.name}")
    return 0


def _run_stage(repo_root: Path, stage: Stage) -> int:
    _print_stage_event(
        stage=stage.label,
        status="running",
        detail=f"timeout={stage.timeout_seconds}s command={_format_command(stage.command)}",
    )

    started = time.perf_counter()
    try:
        completed = subprocess.run(
            list(stage.command),
            cwd=str(repo_root),
            capture_output=True,
            timeout=stage.timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        _print_stage_event(
            stage=stage.label,
            status="failed",
            detail=f"duration_ms={duration_ms} error={exc}",
        )
        return 1
    except subprocess.TimeoutExpired:
        duration_ms = int((time.perf_counter() - started) * 1000)
        _print_stage_event(
            stage=stage.label,
            status="timeout",
            detail=f"duration_ms={duration_ms} timeout={stage.timeout_seconds}s",
        )
        return 1

    duration_ms = int((time.perf_counter() - started) * 1000)
    stdout_text = completed.stdout.decode("utf-8", errors="replace").strip()
    stderr_text = completed.stderr.decode("utf-8", errors="replace").strip()

    if completed.returncode != 0:
        _print_stage_event(
            stage=stage.label,
            status="failed",
            detail=f"duration_ms={duration_ms} exit_code={completed.returncode}",
        )
        if stdout_text:
            print("[verify-container-compose] stdout>")
            print(stdout_text)
        if stderr_text:
            print("[verify-container-compose] stderr>")
            print(stderr_text)
        return completed.returncode

    _print_stage_event(
        stage=stage.label,
        status="passed",
        detail=f"duration_ms={duration_ms} exit_code=0",
    )
    if stdout_text:
        print("[verify-container-compose] stdout>")
        print(stdout_text)
    if stderr_text:
        print("[verify-container-compose] stderr>")
        print(stderr_text)
    return 0


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]

    if _validate_env_contract(repo_root) != 0:
        return 1

    stages: tuple[Stage, ...] = (
        Stage(
            label="build",
            command=("docker", "build", "-t", "schuldockbot:s02", "."),
            timeout_seconds=900,
        ),
        Stage(
            label="run",
            command=(
                "docker",
                "run",
                "--rm",
                "--env-file",
                ".env.example",
                "schuldockbot:s02",
                "--once",
                "--dry-run",
            ),
            timeout_seconds=180,
        ),
        Stage(
            label="compose_config",
            command=("docker", "compose", "--env-file", ".env.example", "config"),
            timeout_seconds=180,
        ),
        Stage(
            label="compose_run",
            command=(
                "docker",
                "compose",
                "--env-file",
                ".env.example",
                "run",
                "--rm",
                "schuldockbot",
                "--once",
                "--dry-run",
            ),
            timeout_seconds=240,
        ),
    )

    for stage in stages:
        exit_code = _run_stage(repo_root, stage)
        if exit_code != 0:
            return exit_code

    _print_stage_event(stage="summary", status="passed", detail="all stages succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
