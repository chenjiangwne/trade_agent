from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger


def _status_path(project_root: Path, config: dict[str, Any]) -> Path:
    return project_root / config["status"]["status_file"]


def _default_status() -> dict[str, Any]:
    return {
        "service_status": "stopped",
        "position_status": "flat",
        "current_phase": "idle",
        "last_processed_1h_bar_time": "",
        "last_processed_4h_bar_time": "",
        "entry_price": 0.0,
        "entry_time": "",
        "last_action": "SKIP",
        "last_score": 0.0,
        "last_entry_score": 0.0,
        "initial_entry_price": 0.0,
        "initial_stop_price": 0.0,
        "peak_rr": 0.0,
    }


def load_status(project_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    path = _status_path(project_root, config)
    if not path.exists():
        default_status = _default_status()
        save_status(project_root, config, default_status)
        logger.warning("status file missing, created default status at {}", path)
        return default_status

    try:
        status = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        default_status = _default_status()
        save_status(project_root, config, default_status)
        logger.error("status file invalid, recreated default status at {}: {}", path, exc)
        return default_status

    status.setdefault("last_processed_1h_bar_time", "")
    status.setdefault("last_processed_4h_bar_time", "")
    status.setdefault("last_entry_score", 0.0)
    status.setdefault("initial_entry_price", float(status.get("entry_price", 0.0) or 0.0))
    status.setdefault("initial_stop_price", 0.0)
    status.setdefault("peak_rr", 0.0)
    return status


def save_status(project_root: Path, config: dict[str, Any], status: dict[str, Any]) -> None:
    path = _status_path(project_root, config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, ensure_ascii=False), encoding="utf-8")
