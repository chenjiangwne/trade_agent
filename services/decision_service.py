from __future__ import annotations

from typing import Any

import pandas as pd

from strategy.FourHour_long import Res, eval_exit, testsuite_result


def make_decision(
    config: dict[str, Any],
    status: dict[str, Any],
    df_4h: pd.DataFrame,
    df_daily: pd.DataFrame,
    latest_bar_time: str,
) -> dict[str, Any]:
    buypoint = config["basic"]["buypoint"]

    if status["position_status"] == "flat":
        result, score, metrics = testsuite_result(df_4h, df_daily)
        action = "BUY" if result == Res["OK"] and score >= buypoint else "HOLD"
        return {
            "action": action,
            "score": float(score),
            "bar_time": latest_bar_time,
            "reason": metrics,
        }

    result, exit_signal = eval_exit(df_4h, float(status["entry_price"]))
    exit_action = _read_exit_action(exit_signal)
    exit_reason = _read_exit_reason(exit_signal)
    should_exit = result == Res["OK"] and exit_action == "EXIT"
    return {
        "action": "EXIT" if should_exit else "HOLD",
        "score": float(status.get("last_score", 0.0)),
        "bar_time": latest_bar_time,
        "reason": exit_reason,
    }


def _read_exit_action(exit_signal: Any) -> str:
    if exit_signal is None:
        return "HOLD"
    if isinstance(exit_signal, dict):
        return str(exit_signal.get("action", "HOLD"))
    strategy_result = getattr(exit_signal, "StrategyResult", None)
    return str(getattr(strategy_result, "value", "HOLD"))


def _read_exit_reason(exit_signal: Any) -> str:
    if exit_signal is None:
        return "no_exit_signal"
    if isinstance(exit_signal, dict):
        return str(exit_signal.get("metric", "no_exit_signal"))
    return str(getattr(exit_signal, "metric", "no_exit_signal"))
