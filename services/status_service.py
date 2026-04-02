from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _status_path(project_root: Path, config: dict[str, Any]) -> Path:
    return project_root / config["status"]["status_file"]


def load_status(project_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    path = _status_path(project_root, config)
    if not path.exists():
        default_status = {
            "service_status": "stopped",
            "position_status": "flat",
            "current_phase": "idle",
            "last_processed_4h_bar_time": "",
            "entry_price": 0.0,
            "entry_time": "",
            "last_action": "SKIP",
            "last_score": 0.0,
        }
        save_status(project_root, config, default_status)
        return default_status

    return json.loads(path.read_text(encoding="utf-8"))


def save_status(project_root: Path, config: dict[str, Any], status: dict[str, Any]) -> None:
    path = _status_path(project_root, config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, ensure_ascii=False), encoding="utf-8")
