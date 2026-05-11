from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger


def _status_path(project_root: Path, config: dict[str, Any]) -> Path:
    return project_root / config["status"]["status_file"]


def _strategy_status_path(project_root: Path, config: dict[str, Any], side: str) -> Path:
    configured = config.get("status", {}).get(f"{side}_status_file")
    if configured:
        return project_root / configured
    return project_root / "config" / f"status_{side}.json"


def _default_status() -> dict[str, Any]:
    return {
        "service_status": "stopped",
        "last_processed_1h_bar_time": "",
        "last_processed_4h_bar_time": "",
        "last_action": "SKIP",
    }


def _default_strategy_status() -> dict[str, Any]:
    return {
        "position_status": "flat",
        "current_phase": "idle",
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

    _ensure_status_shape(status)
    return status


def load_strategy_status(project_root: Path, config: dict[str, Any], side: str, legacy_status: dict[str, Any] | None = None) -> dict[str, Any]:
    path = _strategy_status_path(project_root, config, side)
    if not path.exists():
        default_status = _default_strategy_status()
        if legacy_status:
            default_status.update(_legacy_strategy_status(legacy_status) if side == "short" else {})
            legacy_strategies = legacy_status.get("strategies", {})
            if isinstance(legacy_strategies, dict) and isinstance(legacy_strategies.get(side), dict):
                default_status.update(legacy_strategies[side])
        save_strategy_status(project_root, config, side, default_status)
        logger.warning("{} status file missing, created default status at {}", side, path)
        return default_status

    try:
        status = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        default_status = _default_strategy_status()
        save_strategy_status(project_root, config, side, default_status)
        logger.error("{} status file invalid, recreated default status at {}: {}", side, path, exc)
        return default_status

    _ensure_strategy_status_shape(status)
    return status


def _ensure_status_shape(status: dict[str, Any]) -> None:
    status.setdefault("last_processed_1h_bar_time", "")
    status.setdefault("last_processed_4h_bar_time", "")
    status.setdefault("last_action", "SKIP")


def _legacy_strategy_status(status: dict[str, Any]) -> dict[str, Any]:
    return {
        "position_status": status.get("position_status", "flat"),
        "current_phase": status.get("current_phase", "idle"),
        "entry_price": float(status.get("entry_price", 0.0) or 0.0),
        "entry_time": status.get("entry_time", ""),
        "last_action": status.get("last_action", "SKIP"),
        "last_score": float(status.get("last_score", 0.0) or 0.0),
        "last_entry_score": float(status.get("last_entry_score", 0.0) or 0.0),
        "initial_entry_price": float(status.get("initial_entry_price", status.get("entry_price", 0.0)) or 0.0),
        "initial_stop_price": float(status.get("initial_stop_price", 0.0) or 0.0),
        "peak_rr": float(status.get("peak_rr", 0.0) or 0.0),
    }


def _ensure_strategy_status_shape(status: dict[str, Any]) -> None:
    defaults = _default_strategy_status()
    for key, value in defaults.items():
        status.setdefault(key, value)


def save_status(project_root: Path, config: dict[str, Any], status: dict[str, Any]) -> None:
    path = _status_path(project_root, config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, ensure_ascii=False), encoding="utf-8")


def save_strategy_status(project_root: Path, config: dict[str, Any], side: str, status: dict[str, Any]) -> None:
    path = _strategy_status_path(project_root, config, side)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, ensure_ascii=False), encoding="utf-8")
