from __future__ import annotations

from pathlib import Path
import re

WORKFLOW_PATH = Path(".github/workflows/publish-ghcr.yml")
EXPECTED_IMAGE = "ghcr.io/borega/schuldockbot:latest"


def _read_required(path: Path) -> str:
    assert path.exists(), (
        f"{path} must exist so GHCR publish semantics can be contract-tested"
    )
    return path.read_text(encoding="utf-8")


def _normalized_yaml(content: str) -> str:
    return re.sub(r"\s+", " ", content).strip()


def _line_indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _block_end(lines: list[str], start_index: int, base_indent: int) -> int:
    for index in range(start_index + 1, len(lines)):
        candidate = lines[index]
        if not candidate.strip():
            continue
        if _line_indent(candidate) <= base_indent:
            return index
    return len(lines)


def _extract_push_branches(workflow: str) -> list[str]:
    lines = workflow.splitlines()

    on_index = next(
        (i for i, line in enumerate(lines) if line.strip() == "on:"),
        None,
    )
    assert on_index is not None, "Missing top-level workflow trigger block: expected `on:`"

    on_indent = _line_indent(lines[on_index])
    on_end = _block_end(lines, on_index, on_indent)

    push_index = next(
        (
            i
            for i in range(on_index + 1, on_end)
            if lines[i].strip() == "push:" and _line_indent(lines[i]) > on_indent
        ),
        None,
    )
    assert push_index is not None, "Missing `on.push` trigger block"

    push_indent = _line_indent(lines[push_index])
    push_end = _block_end(lines, push_index, push_indent)

    branches_index = next(
        (
            i
            for i in range(push_index + 1, push_end)
            if lines[i].strip().startswith("branches:")
            and _line_indent(lines[i]) > push_indent
        ),
        None,
    )
    assert branches_index is not None, "Missing `on.push.branches` contract fragment"

    branches_line = lines[branches_index]
    _, _, remainder = branches_line.partition(":")
    inline_value = remainder.strip()

    if inline_value:
        assert inline_value.startswith("[") and inline_value.endswith("]"), (
            "Unsupported `on.push.branches` format. Use YAML list style or inline "
            "list syntax."
        )
        items = [item.strip() for item in inline_value[1:-1].split(",") if item.strip()]
        return [item.strip("\"'") for item in items]

    branches_indent = _line_indent(branches_line)
    branches: list[str] = []

    for index in range(branches_index + 1, push_end):
        line = lines[index]
        if not line.strip():
            continue
        if _line_indent(line) <= branches_indent:
            break

        stripped = line.strip()
        if stripped.startswith("- "):
            branches.append(stripped[2:].strip().strip("\"'"))

    assert branches, "`on.push.branches` must include at least one branch entry"
    return branches


def _assert_exact_mapping_value(workflow: str, key: str, value: str, context: str) -> None:
    pattern = re.compile(
        rf"(?m)^\s*{re.escape(key)}\s*:\s*{re.escape(value)}\s*$"
    )
    assert pattern.search(workflow), (
        f"Missing `{key}: {value}` in {context}; publish workflow contract drifted"
    )


def test_publish_workflow_exists_for_contract_enforcement() -> None:
    _read_required(WORKFLOW_PATH)


def test_publish_workflow_triggers_on_main_pushes_only() -> None:
    workflow = _read_required(WORKFLOW_PATH)

    branches = _extract_push_branches(workflow)
    assert branches == ["main"], (
        "Publish workflow must trigger only on `main` pushes. "
        f"Found on.push.branches={branches!r}"
    )


def test_publish_workflow_declares_explicit_package_write_permissions() -> None:
    workflow = _read_required(WORKFLOW_PATH)

    _assert_exact_mapping_value(
        workflow,
        key="contents",
        value="read",
        context="workflow permissions",
    )
    _assert_exact_mapping_value(
        workflow,
        key="packages",
        value="write",
        context="workflow permissions",
    )


def test_publish_workflow_includes_required_actions_and_ghcr_login_wiring() -> None:
    workflow = _read_required(WORKFLOW_PATH)
    normalized = _normalized_yaml(workflow)

    assert re.search(r"(?m)^\s*(?:-\s*)?uses:\s*actions/checkout@", workflow), (
        "Missing `actions/checkout` step in publish workflow"
    )
    assert re.search(r"(?m)^\s*(?:-\s*)?uses:\s*docker/login-action@", workflow), (
        "Missing `docker/login-action` step in publish workflow"
    )
    assert "registry: ghcr.io" in normalized, (
        "GHCR login step must target `registry: ghcr.io`"
    )
    assert re.search(
        r"(?m)^\s*username:\s*\$\{\{\s*github\.actor\s*\}\}\s*$", workflow
    ), "GHCR login must use `${{ github.actor }}` as username"
    assert re.search(
        r"(?m)^\s*password:\s*\$\{\{\s*secrets\.GITHUB_TOKEN\s*\}\}\s*$",
        workflow,
    ), "GHCR login must use `${{ secrets.GITHUB_TOKEN }}` as password"


def test_publish_workflow_build_push_targets_latest_image_and_enables_push() -> None:
    workflow = _read_required(WORKFLOW_PATH)

    assert re.search(r"(?m)^\s*(?:-\s*)?uses:\s*docker/build-push-action@", workflow), (
        "Missing `docker/build-push-action` step in publish workflow"
    )
    assert EXPECTED_IMAGE in workflow, (
        "Build/push step must publish exact target "
        f"`{EXPECTED_IMAGE}`"
    )
    assert re.search(r"(?m)^\s*push:\s*true\s*$", workflow), (
        "Build/push step must set `push: true`"
    )
