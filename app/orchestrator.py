from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from loguru import logger

from services.decision_service import make_decision
from services.execution_service import execute_order
from services.market_data_service import load_market_data
from services.push_message import push_failure_message, push_message
from services.status_service import load_status, load_strategy_status, save_status, save_strategy_status


SIDES = ("short", "long")


def run_once(project_root: Path, config: dict[str, Any], force_process: bool = False) -> dict[str, Any]:
    status = load_status(project_root, config)
    enabled_sides = _enabled_sides(config)
    strategy_statuses = {
        side: load_strategy_status(project_root, config, side, legacy_status=status)
        for side in enabled_sides
    }
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
        for side in enabled_sides:
            strategy_statuses[side]["last_action"] = "SKIP"
            save_strategy_status(project_root, config, side, strategy_statuses[side])
        save_status(project_root, config, status)
        return status

    if force_process:
        logger.info("force_process enabled for this run; decision flow will execute regardless of last_processed_4h_bar_time")

    current_close = float(df_4h.iloc[-1]["close"])
    current_time = str(df_4h.iloc[-1]["timestamp"].isoformat())
    side_results: dict[str, dict[str, Any]] = {}

    if not enabled_sides:
        logger.warning("no main strategies enabled; skip decision flow")
        status["service_status"] = "running"
        status["last_action"] = "SKIP"
        status["last_processed_1h_bar_time"] = latest_1h_bar_time
        status["last_processed_4h_bar_time"] = latest_4h_bar_time
        save_status(project_root, config, status)
        return status

    with ThreadPoolExecutor(max_workers=len(enabled_sides), thread_name_prefix="strategy") as executor:
        futures = {
            executor.submit(
                _run_side,
                side,
                config,
                strategy_statuses[side].copy(),
                df_1h,
                df_4h,
                df_daily,
                latest_4h_bar_time,
                current_close,
                current_time,
            ): side
            for side in SIDES
            if side in enabled_sides
        }
        for future in as_completed(futures):
            side = futures[future]
            try:
                side_results[side] = future.result()
            except Exception as exc:
                logger.opt(colors=True).exception(f"{_log_title(side)} strategy worker failed")
                side_results[side] = {
                    "decision": {"side": side, "action": "ERROR", "score": 0.0, "reason": repr(exc)},
                    "status_updates": {
                        "last_action": "ERROR",
                        "last_score": 0.0,
                        "current_phase": "error",
                    },
                }
                push_failure_message(
                    config,
                    title=f"trade_agent {side} strategy worker failed",
                    content=repr(exc),
                )

    for side, result in side_results.items():
        strategy_statuses[side].update(result["status_updates"])
        save_strategy_status(project_root, config, side, strategy_statuses[side])

    status["service_status"] = "running"
    status["last_action"] = _combined_last_action(side_results)
    status["last_processed_1h_bar_time"] = latest_1h_bar_time
    status["last_processed_4h_bar_time"] = latest_4h_bar_time
    save_status(project_root, config, status)

    for side in SIDES:
        summary = _decision_summary(side_results.get(side, {}).get("decision")) if side in enabled_sides else "disabled"
        logger.opt(colors=True).info(f"{_log_title(side)} decision summary: {{}}", summary)
    return status


def _enabled_sides(config: dict[str, Any]) -> tuple[str, ...]:
    enabled = config.get("strategy", {}).get("enabled", {})
    if not isinstance(enabled, dict):
        return SIDES
    return tuple(side for side in SIDES if bool(enabled.get(side, True)))


def _run_side(
    side: str,
    config: dict[str, Any],
    side_status: dict[str, Any],
    df_1h,
    df_4h,
    df_daily,
    latest_4h_bar_time: str,
    current_close: float,
    current_time: str,
) -> dict[str, Any]:
    logger.opt(colors=True).info(f"{_log_title(side)} worker start")
    decision = make_decision(config, side_status, df_1h, df_4h, df_daily, latest_4h_bar_time, side=side)
    _push_decision_message(config, side, side_status, decision, current_close, current_time)
    execution_result = execute_order(config, side_status, decision, df_4h, side=side)
    logger.opt(colors=True).info(f"{_log_title(side)} worker done action={{}} score={{}}", decision["action"], decision["score"])
    return {
        "decision": decision,
        "status_updates": execution_result["status_updates"],
        "live_order": execution_result.get("live_order"),
    }


def _push_decision_message(
    config: dict[str, Any],
    side: str,
    side_status: dict[str, Any],
    decision: dict[str, Any],
    current_close: float,
    current_time: str,
) -> None:
    action = decision["action"]
    if action == "HOLD":
        return

    title = _message_title(side, action)
    decision_state_updates = decision.get("status_updates", {}) if isinstance(decision, dict) else {}
    initial_entry_price = float(decision_state_updates.get("initial_entry_price", current_close))
    initial_stop_price = float(decision_state_updates.get("initial_stop_price", 0.0))
    exit_price = float(decision.get("exit_price", current_close))

    content_lines = [
        f"symbol={config['basic']['symbol']}",
        f"side={side}",
        f"signal_time={current_time}",
        f"bar_time={decision['bar_time']}",
        f"current_close={current_close}",
        f"score={decision['score']}",
        f"reason={decision.get('reason', 'no_metric')}",
    ]

    if action in {"SHORT", "ADD_SHORT", "LONG", "ADD_LONG"}:
        content_lines.extend(
            [
                f"entry_price={side_status.get('entry_price', current_close) if action.startswith('ADD_') else current_close}",
                f"initial_entry_price={initial_entry_price}",
                f"initial_stop_price={initial_stop_price}",
            ]
        )
    elif action == "EXIT":
        content_lines.extend(
            [
                f"entry_price={side_status.get('entry_price', '')}",
                f"exit_price={exit_price}",
            ]
        )

    push_message(config, title=title, content="\n".join(content_lines))


def _message_title(side: str, action: str) -> str:
    if action in {"SHORT", "LONG"}:
        return f"trade_agent {side} entry signal"
    if action in {"ADD_SHORT", "ADD_LONG"}:
        return f"trade_agent add {side} signal"
    if action == "EXIT":
        return f"trade_agent {side} exit signal"
    return f"trade_agent {side} {action.lower()} signal"


def _log_title(side: str) -> str:
    return "<green>[LONG]</green>" if side == "long" else "<red>[SHORT]</red>"


def _decision_summary(decision: dict[str, Any] | None) -> str:
    if not decision:
        return "missing"
    return f"{decision.get('action')} score={decision.get('score')} reason={decision.get('reason', 'no_metric')}"


def _combined_last_action(side_results: dict[str, dict[str, Any]]) -> str:
    actions = [
        result.get("decision", {}).get("action", "SKIP")
        for result in side_results.values()
    ]
    if any(action not in {"HOLD", "SKIP"} for action in actions):
        return ",".join(action for action in actions if action not in {"HOLD", "SKIP"})
    if actions:
        return ",".join(actions)
    return "SKIP"
