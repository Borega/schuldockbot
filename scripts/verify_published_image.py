from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

DEFAULT_IMAGE = "ghcr.io/borega/schuldockbot:latest"
DEFAULT_ENV_FILE = ".env.example"

REQUIRED_ENV_KEYS: tuple[str, ...] = (
    "SCHULDOCKBOT_SOURCE_JSON_URL",
    "SCHULDOCKBOT_SOURCE_HTML_URL",
    "SCHULDOCKBOT_STATE_DB_PATH",
    "SCHULDOCKBOT_TALK_BASE_URL",
    "SCHULDOCKBOT_TALK_ROOM_TOKEN",
    "SCHULDOCKBOT_TALK_USERNAME",
    "SCHULDOCKBOT_TALK_APP_PASSWORD",
)

REQUIRED_LIFECYCLE_EVENTS: tuple[str, ...] = (
    "startup",
    "shutdown",
)


@dataclass(frozen=True)
class Stage:
    label: str
    command: tuple[str, ...]
    timeout_seconds: int


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    stdout_text: str
    stderr_text: str


def _format_command(command: tuple[str, ...]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def _print_stage_event(*, stage: str, status: str, detail: str | None = None) -> None:
    message = f"[verify-published-image] stage={stage} status={status}"
    if detail:
        message = f"{message} detail={detail}"
    print(message)


def _collect_env_keys_and_issues(env_file: Path) -> tuple[dict[str, str], list[str]]:
    entries: dict[str, str] = {}
    issues: list[str] = []

    for line_number, raw_line in enumerate(
        env_file.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        key, separator, value = line.partition("=")
        if not separator:
            issues.append(f"line_{line_number}:missing_equals")
            continue

        normalized_key = key.strip()
        normalized_value = value.strip()

        if not normalized_key:
            issues.append(f"line_{line_number}:missing_key")
            continue

        entries[normalized_key] = normalized_value

    return entries, issues


def _validate_env_contract(repo_root: Path, env_file_relative: str) -> int:
    stage = "env_contract"
    env_path = (repo_root / env_file_relative).resolve()

    if not env_path.exists():
        _print_stage_event(
            stage=stage,
            status="failed",
            detail=f"missing_env_file={env_file_relative}",
        )
        return 1

    env_values, issues = _collect_env_keys_and_issues(env_path)
    missing = [key for key in REQUIRED_ENV_KEYS if key not in env_values]
    blank = [key for key in REQUIRED_ENV_KEYS if not env_values.get(key, "").strip()]

    if issues or missing or blank:
        details: list[str] = []
        if issues:
            details.append(f"invalid_entries={','.join(issues)}")
        if missing:
            details.append(f"missing_keys={','.join(missing)}")
        if blank:
            details.append(f"blank_keys={','.join(blank)}")

        _print_stage_event(stage=stage, status="failed", detail=";".join(details))
        return 1

    _print_stage_event(
        stage=stage,
        status="passed",
        detail=f"env_file={env_file_relative}",
    )
    return 0


def _validate_image_reference(image: str) -> str | None:
    if not image.strip():
        return "image reference is empty"

    if re.search(r"\s", image):
        return "image reference contains whitespace"

    if image.startswith(("/", ":", "@")):
        return "image reference has invalid leading character"

    trailing_segment = image.rsplit("/", maxsplit=1)[-1]
    if ":" not in trailing_segment and "@" not in trailing_segment:
        return "image reference must include an explicit tag or digest"

    return None


def _run_stage(repo_root: Path, stage: Stage) -> CommandResult | None:
    _print_stage_event(
        stage=stage.label,
        status="running",
        detail=(
            f"timeout={stage.timeout_seconds}s "
            f"command={_format_command(stage.command)}"
        ),
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
        return None
    except subprocess.TimeoutExpired:
        duration_ms = int((time.perf_counter() - started) * 1000)
        _print_stage_event(
            stage=stage.label,
            status="timeout",
            detail=f"duration_ms={duration_ms} timeout={stage.timeout_seconds}s",
        )
        return None

    duration_ms = int((time.perf_counter() - started) * 1000)
    stdout_text = completed.stdout.decode("utf-8", errors="replace").strip()
    stderr_text = completed.stderr.decode("utf-8", errors="replace").strip()

    if completed.returncode != 0:
        _print_stage_event(
            stage=stage.label,
            status="failed",
            detail=f"duration_ms={duration_ms} exit_code={completed.returncode}",
        )
    else:
        _print_stage_event(
            stage=stage.label,
            status="passed",
            detail=f"duration_ms={duration_ms} exit_code=0",
        )

    if stdout_text:
        print("[verify-published-image] stdout>")
        print(stdout_text)
    if stderr_text:
        print("[verify-published-image] stderr>")
        print(stderr_text)

    return CommandResult(
        exit_code=completed.returncode,
        stdout_text=stdout_text,
        stderr_text=stderr_text,
    )


def _extract_lifecycle_events(*, stdout_text: str, stderr_text: str) -> set[str]:
    events: set[str] = set()

    combined = "\n".join(part for part in (stdout_text, stderr_text) if part)
    for line in combined.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if not candidate.startswith("{"):
            continue

        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue

        event = payload.get("event")
        if isinstance(event, str) and event:
            events.add(event)

    return events


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="verify-published-image",
        description=(
            "Verify GHCR image pull/run lifecycle evidence with stage-labeled diagnostics."
        ),
    )
    parser.add_argument(
        "--image",
        default=DEFAULT_IMAGE,
        help=f"Image reference to verify (default: {DEFAULT_IMAGE}).",
    )
    parser.add_argument(
        "--env-file",
        default=DEFAULT_ENV_FILE,
        help=f"Env file path relative to repository root (default: {DEFAULT_ENV_FILE}).",
    )
    parser.add_argument(
        "--skip-pull",
        action="store_true",
        help="Skip docker pull stage (local fallback mode for already-built images).",
    )

    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    image_error = _validate_image_reference(args.image)
    if image_error is not None:
        _print_stage_event(
            stage="image_reference",
            status="failed",
            detail=image_error,
        )
        return 1

    _print_stage_event(
        stage="image_reference",
        status="passed",
        detail=f"image={args.image}",
    )

    if _validate_env_contract(repo_root, args.env_file) != 0:
        return 1

    docker_cli_stage = Stage(
        label="docker_cli",
        command=("docker", "--version"),
        timeout_seconds=30,
    )
    docker_cli = _run_stage(repo_root, docker_cli_stage)
    if docker_cli is None or docker_cli.exit_code != 0:
        return 1

    docker_daemon_stage = Stage(
        label="docker_daemon",
        command=("docker", "info", "--format", "{{.ServerVersion}}"),
        timeout_seconds=30,
    )
    docker_daemon = _run_stage(repo_root, docker_daemon_stage)
    if docker_daemon is None or docker_daemon.exit_code != 0:
        return 1

    if not args.skip_pull:
        pull_stage = Stage(
            label="pull_image",
            command=("docker", "pull", args.image),
            timeout_seconds=420,
        )
        pull_result = _run_stage(repo_root, pull_stage)
        if pull_result is None or pull_result.exit_code != 0:
            return 1
    else:
        _print_stage_event(
            stage="pull_image",
            status="skipped",
            detail="skip_pull=true",
        )

    run_stage = Stage(
        label="run_container",
        command=(
            "docker",
            "run",
            "--rm",
            "--env-file",
            args.env_file,
            args.image,
            "--once",
            "--dry-run",
        ),
        timeout_seconds=240,
    )
    run_result = _run_stage(repo_root, run_stage)
    if run_result is None:
        return 1
    if run_result.exit_code != 0:
        return run_result.exit_code

    events = _extract_lifecycle_events(
        stdout_text=run_result.stdout_text,
        stderr_text=run_result.stderr_text,
    )
    missing_events = [event for event in REQUIRED_LIFECYCLE_EVENTS if event not in events]
    if missing_events:
        _print_stage_event(
            stage="lifecycle_contract",
            status="failed",
            detail=f"missing_events={','.join(missing_events)}",
        )
        return 1

    _print_stage_event(
        stage="lifecycle_contract",
        status="passed",
        detail=f"events={','.join(sorted(events))}",
    )
    _print_stage_event(stage="summary", status="passed", detail="all stages succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
