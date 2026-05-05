from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from services.decision_service import make_decision
from services.execution_service import execute_order
from services.market_data_service import load_market_data
from services.push_message import push_failure_message, push_message
from services.status_service import load_status, save_status


def run_once(project_root: Path, config: dict[str, Any], force_process: bool = False) -> dict[str, Any]:
    status = load_status(project_root, config)
    try:
        df_1h, df_4h, df_daily, latest_1h_bar_time, latest_4h_bar_time = load_market_data(project_root, config)
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
        "validated data window: 1h_rows={} 4h_rows={} 1d_rows={} latest_1h_bar={} latest_4h_bar={}",
        len(df_1h),
        len(df_4h),
        len(df_daily),
        latest_1h_bar_time,
        latest_4h_bar_time,
    )

    if not force_process and status.get("last_processed_4h_bar_time", "") == latest_4h_bar_time:
        logger.info("4h bar already processed, skip this round")
        status["last_action"] = "SKIP"
        save_status(project_root, config, status)
        return status

    if force_process:
        logger.info("force_process enabled for this run; decision flow will execute regardless of last_processed_4h_bar_time")

    decision = make_decision(config, status, df_1h, df_4h, df_daily, latest_4h_bar_time)
    current_close = float(df_4h.iloc[-1]["close"])
    current_time = str(df_4h.iloc[-1]["timestamp"].isoformat())
    exit_price = float(decision.get("exit_price", current_close))
    decision_state_updates = decision.get("status_updates", {}) if isinstance(decision, dict) else {}
    initial_entry_price = float(decision_state_updates.get("initial_entry_price", current_close))
    initial_stop_price = float(decision_state_updates.get("initial_stop_price", 0.0))
    if decision["action"] == "SHORT":
        push_message(
            config,
            title="trade_agent short entry signal",
            content=(
                f"symbol={config['basic']['symbol']}\n"
                f"signal_time={current_time}\n"
                f"bar_time={decision['bar_time']}\n"
                f"entry_price={current_close}\n"
                f"initial_entry_price={initial_entry_price}\n"
                f"initial_stop_price={initial_stop_price}\n"
                f"current_close={current_close}\n"
                f"score={decision['score']}\n"
                f"reason={decision.get('reason', 'no_metric')}"
            ),
        )
    elif decision["action"] == "ADD_SHORT":
        push_message(
            config,
            title="trade_agent add short signal",
            content=(
                f"symbol={config['basic']['symbol']}\n"
                f"signal_time={current_time}\n"
                f"bar_time={decision['bar_time']}\n"
                f"entry_price={status.get('entry_price', '')}\n"
                f"initial_entry_price={initial_entry_price}\n"
                f"initial_stop_price={initial_stop_price}\n"
                f"current_close={current_close}\n"
                f"score={decision['score']}\n"
                f"reason={decision.get('reason', 'no_metric')}"
            ),
        )
    elif decision["action"] == "EXIT":
        push_message(
            config,
            title="trade_agent exit signal",
            content=(
                f"symbol={config['basic']['symbol']}\n"
                f"signal_time={current_time}\n"
                f"bar_time={decision['bar_time']}\n"
                f"entry_price={status.get('entry_price', '')}\n"
                f"exit_price={exit_price}\n"
                f"current_close={current_close}\n"
                f"score={decision['score']}\n"
                f"reason={decision.get('reason', 'no_metric')}"
            ),
        )
    execution_result = execute_order(config, status, decision, df_4h)

    status.update(execution_result["status_updates"])
    status["service_status"] = "running"
    status["last_processed_1h_bar_time"] = latest_1h_bar_time
    status["last_processed_4h_bar_time"] = latest_4h_bar_time
    save_status(project_root, config, status)

    logger.info(
        "decision={} score={} metrics={}",
        decision["action"],
        decision["score"],
        decision.get("reason", "no_metric"),
    )
    return status
