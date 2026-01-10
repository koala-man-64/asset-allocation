import os
import sys
from pathlib import Path

import uvicorn


def _ensure_repo_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def main_loop() -> None:
    _ensure_repo_on_path()
    host = os.environ.get("UI_HOST", "0.0.0.0")
    port = int(os.environ.get("UI_PORT", "8001"))
    uvicorn.run("asset_allocation.ui.web:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main_loop()
