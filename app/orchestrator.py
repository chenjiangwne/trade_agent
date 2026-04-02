from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from services.decision_service import make_decision
from services.execution_service import execute_order
from services.market_data_service import load_market_data
from services.status_service import load_status, save_status


def run_once(project_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    status = load_status(project_root, config)
    df_4h, df_daily, latest_bar_time = load_market_data(project_root, config)

    if status["last_processed_4h_bar_time"] == latest_bar_time:
        logger.info("4h bar already processed, skip this round")
        status["last_action"] = "SKIP"
        save_status(project_root, config, status)
        return status

    decision = make_decision(config, status, df_4h, df_daily, latest_bar_time)
    execution_result = execute_order(config, status, decision, df_4h)

    status.update(execution_result["status_updates"])
    status["service_status"] = "running"
    status["last_processed_4h_bar_time"] = latest_bar_time
    save_status(project_root, config, status)

    logger.info("decision={} score={} reason={}", decision["action"], decision["score"], decision["reason"])
    return status
