from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from services.decision_service import make_decision
from services.execution_service import execute_order
from services.market_data_service import load_market_data
from services.push_message import push_failure_message
from services.status_service import load_status, save_status


def run_once(project_root: Path, config: dict[str, Any], force_process: bool = False) -> dict[str, Any]:
    status = load_status(project_root, config)
    try:
        df_4h, df_daily, latest_bar_time = load_market_data(project_root, config)
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

    logger.info("validated data window: 4h_rows={} 1d_rows={} latest_4h_bar={}", len(df_4h), len(df_daily), latest_bar_time)

    if not force_process and status["last_processed_4h_bar_time"] == latest_bar_time:
        logger.info("4h bar already processed, skip this round")
        status["last_action"] = "SKIP"
        save_status(project_root, config, status)
        return status

    if force_process:
        logger.info("force_process enabled for this run; decision flow will execute regardless of last_processed_4h_bar_time")

    decision = make_decision(config, status, df_4h, df_daily, latest_bar_time)
    execution_result = execute_order(config, status, decision, df_4h)

    status.update(execution_result["status_updates"])
    status["service_status"] = "running"
    status["last_processed_4h_bar_time"] = latest_bar_time
    save_status(project_root, config, status)

    logger.info(
        "decision={} score={} metrics={}",
        decision["action"],
        decision["score"],
        decision.get("reason", "no_metric"),
    )
    return status
