from __future__ import annotations

from pathlib import Path
import re

DOCKERFILE_PATH = Path("Dockerfile")
DOCKERIGNORE_PATH = Path(".dockerignore")


def _read_required(path: Path) -> str:
    assert path.exists(), f"{path} must exist"
    return path.read_text(encoding="utf-8")


def _normalized_dockerfile(content: str) -> str:
    without_line_continuations = re.sub(r"\\\s*\n", " ", content)
    return re.sub(r"\s+", " ", without_line_continuations).strip()


def _dockerignore_patterns() -> set[str]:
    patterns: set[str] = set()

    for raw_line in _read_required(DOCKERIGNORE_PATH).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        patterns.add(line.rstrip("/"))

    return patterns


def test_dockerfile_uses_python_3_11_slim_base_image() -> None:
    dockerfile = _read_required(DOCKERFILE_PATH)

    assert re.search(
        r"^FROM\s+python:3\.11-slim(?:\s+AS\s+\w+)?\s*$",
        dockerfile,
        flags=re.IGNORECASE | re.MULTILINE,
    )


def test_dockerfile_installs_project_with_pip_install_dot_contract() -> None:
    dockerfile = _normalized_dockerfile(_read_required(DOCKERFILE_PATH))

    assert "python -m pip install ." in dockerfile


def test_dockerfile_entrypoint_uses_real_runtime_module_without_smoke_defaults() -> None:
    dockerfile = _read_required(DOCKERFILE_PATH)

    assert re.search(
        r'^ENTRYPOINT\s+\[\s*"python"\s*,\s*"-m"\s*,\s*"schuldockbot\.runtime\.main"\s*\]\s*$',
        dockerfile,
        flags=re.MULTILINE,
    )
    assert "--once" not in dockerfile
    assert "--dry-run" not in dockerfile


def test_dockerignore_excludes_high_risk_build_context_paths() -> None:
    patterns = _dockerignore_patterns()
    required = {
        ".git",
        ".gsd*",
        ".pytest-tmp*",
        ".pytest_tmp_state*",
        ".pytest_cache",
        "__pycache__",
        ".env",
        ".env.*",
    }

    missing = sorted(required - patterns)
    assert not missing, f"Missing required .dockerignore entries: {', '.join(missing)}"
