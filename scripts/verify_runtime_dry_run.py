from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    state_db_path = repo_root / ".pytest-tmp" / "s05-proof" / "runtime-cli.db"
    state_db_path.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = "src" if not existing_pythonpath else f"src{os.pathsep}{existing_pythonpath}"
    env.update(
        {
            "SCHULDOCKBOT_SOURCE_JSON_URL": "https://schuldock.hamburg/wp-json/schuldock/v1/issues",
            "SCHULDOCKBOT_SOURCE_HTML_URL": "https://schuldock.hamburg/aktuelle-meldungen",
            "SCHULDOCKBOT_STATE_DB_PATH": str(state_db_path),
            "SCHULDOCKBOT_TALK_BASE_URL": "https://cloud.example",
            "SCHULDOCKBOT_TALK_ROOM_TOKEN": "room-token-placeholder",
            "SCHULDOCKBOT_TALK_USERNAME": "bot-user",
            "SCHULDOCKBOT_TALK_APP_PASSWORD": "app-password-placeholder",
        }
    )

    return subprocess.call(
        [sys.executable, "-m", "schuldockbot.runtime.main", "--once", "--dry-run"],
        cwd=str(repo_root),
        env=env,
    )


if __name__ == "__main__":
    raise SystemExit(main())
