from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from services.decision_service import make_decision
from services.execution_service import execute_order
from services.market_data_service import load_market_data
from services.push_message import push_failure_message, push_message
from services.short_4h_service import make_short_4h_decision
from services.status_service import load_status, save_status


def _signal_csv_path(project_root: Path, config: dict[str, Any]) -> Path:
    reports_dir = project_root / config.get("log", {}).get("report_dir", "reports")
    return reports_dir / "trade_signals.csv"


def _append_signal_record(
    project_root: Path,
    config: dict[str, Any],
    signal_time: str,
    entry_price: float | str,
    exit_price: float | str,
) -> None:
    path = _signal_csv_path(project_root, config)
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open("a", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        if not file_exists:
            writer.writerow(["time", "entry_price", "exit_price"])
        writer.writerow([signal_time, entry_price, exit_price])


def run_once(project_root: Path, config: dict[str, Any], force_process: bool = False) -> dict[str, Any]:
    status = load_status(project_root, config)
    try:
        df_1h, df_4h, df_daily, latest_bar_time = load_market_data(project_root, config)
    except Exception as exc:
        status["service_status"] = "error"
        status["last_action"] = "SKIP"
        save_status(project_root, config, status)
        push_failure_message(
            config,
            title="trade_agent market data validation failed",
            content=str(exc),
        )
        raise

    logger.info(
        "validated data window: 1h_rows={} 4h_rows={} 1d_rows={} latest_1h_bar={}",
        len(df_1h),
        len(df_4h),
        len(df_daily),
        latest_bar_time,
    )

    short_enabled = bool(config.get("strategy", {}).get("short", False))
    latest_4h_bar_time = str(pd.to_datetime(df_4h.iloc[-1]["timestamp"]).isoformat())
    process_bar_time = latest_4h_bar_time if short_enabled else latest_bar_time
    processed_key = "last_processed_4h_bar_time" if short_enabled else "last_processed_1h_bar_time"

    if not force_process and status.get(processed_key, "") == process_bar_time:
        logger.info("{} bar already processed, skip this round", "4h" if short_enabled else "1h")
        status["last_action"] = "SKIP"
        save_status(project_root, config, status)
        return status

    if force_process:
        logger.info("force_process enabled for this run; decision flow will execute regardless of last_processed_4h_bar_time")

    if short_enabled:
        decision = make_short_4h_decision(config, status, df_1h, df_4h, df_daily)
        execution_frame = df_4h
    else:
        decision = make_decision(config, status, df_1h, df_4h, df_daily, latest_bar_time)
        execution_frame = df_1h

    current_price = float(execution_frame.iloc[-1]["close"])
    current_bar_time = str(pd.to_datetime(execution_frame.iloc[-1]["timestamp"]).isoformat())
    if decision["action"] in {"SHORT", "LONG"}:
        action_label = decision["action"].lower()
        push_message(
            config,
            title=f"trade_agent {action_label} signal",
            content=(
                f"symbol={config['basic']['symbol']}\n"
                f"strategy={decision.get('strategy', action_label)}\n"
                f"signal_time={current_bar_time}\n"
                f"bar_time={decision['bar_time']}\n"
                f"current_close={current_price}\n"
                f"entry_price={current_price}\n"
                f"score={decision['score']}\n"
                f"reason={decision.get('reason', 'no_metric')}"
            ),
        )
        _append_signal_record(project_root, config, current_bar_time, current_price, "")
    elif decision["action"] == "EXIT":
        entry_price = status.get("entry_price", "")
        push_message(
            config,
            title="trade_agent exit signal",
            content=(
                f"symbol={config['basic']['symbol']}\n"
                f"strategy={decision.get('strategy', 'unknown')}\n"
                f"signal_time={current_bar_time}\n"
                f"bar_time={decision['bar_time']}\n"
                f"current_close={current_price}\n"
                f"entry_price={entry_price}\n"
                f"exit_price={current_price}\n"
                f"score={decision['score']}\n"
                f"reason={decision.get('reason', 'no_metric')}"
            ),
        )
        _append_signal_record(project_root, config, current_bar_time, entry_price, current_price)
    execution_result = execute_order(config, status, decision, execution_frame)

    status.update(execution_result["status_updates"])
    status["service_status"] = "running"
    status["last_processed_1h_bar_time"] = latest_bar_time
    status["last_processed_4h_bar_time"] = latest_4h_bar_time
    status["cooldown_count"] = int(decision.get("cooldown_count", status.get("cooldown_count", 0) or 0))
    save_status(project_root, config, status)

    logger.info(
        "decision={} score={} metrics={}",
        decision["action"],
        decision["score"],
        decision.get("reason", "no_metric"),
    )
    return status
