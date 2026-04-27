from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger

from strategy.FourHour_short import Res, eval_exit, testsuite_result


def short_runtime_config(config: dict[str, Any]) -> dict[str, Any]:
    backtest_short = config.get("backtest", {}).get("short", {})
    strategy_short = config.get("strategy", {}).get("short_config", {})
    return {
        "buypoint": int(strategy_short.get("buypoint", backtest_short.get("buypoint", config["basic"]["buypoint"]))),
        "cooldown_count": int(backtest_short.get("cooldown_count", config["basic"].get("cooldown_count", 0))),
        "eval_exit_enabled": bool(backtest_short.get("eval_exit", {}).get("enabled", True)),
    }


def make_short_4h_decision(
    config: dict[str, Any],
    status: dict[str, Any],
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_daily: pd.DataFrame,
) -> dict[str, Any]:
    runtime = short_runtime_config(config)
    latest_4h_time = str(pd.to_datetime(df_4h.iloc[-1]["timestamp"]).isoformat())
    current_price = float(df_4h.iloc[-1]["close"])

    cooldown_count = int(status.get("cooldown_count", 0) or 0)
    if cooldown_count > 0:
        return {
            "action": "HOLD",
            "score": float(status.get("last_score", 0.0)),
            "bar_time": latest_4h_time,
            "reason": f"short 4h cooldown remaining={cooldown_count}",
            "metrics": [f"cooldown_count={cooldown_count}"],
            "strategy": "short",
            "cooldown_count": cooldown_count - 1,
        }

    result, score, metrics = testsuite_result(df_1h, df_4h, df_daily)
    metric_text = _normalize_reason(metrics)
    if result != Res["OK"]:
        logger.error("short 4h entry scoring failed: result={} score={} metrics={}", result, score, metric_text)
        return _hold_decision(status, latest_4h_time, score, metric_text)

    if score >= runtime["buypoint"]:
        return {
            "action": "SHORT",
            "score": float(score),
            "bar_time": latest_4h_time,
            "reason": metric_text,
            "metrics": metrics if isinstance(metrics, list) else [metric_text],
            "strategy": "short",
            "cooldown_count": runtime["cooldown_count"],
            "signal_price": current_price,
        }

    if status.get("position_status") == "short" and runtime["eval_exit_enabled"]:
        exit_result, exit_signal = eval_exit(df_1h, df_4h, float(status.get("entry_price", 0.0)))
        exit_action = _read_exit_action(exit_signal)
        exit_reason = _read_exit_reason(exit_signal)
        if exit_result == Res["OK"] and exit_action == "EXIT":
            return {
                "action": "EXIT",
                "score": float(status.get("last_score", score)),
                "bar_time": latest_4h_time,
                "reason": exit_reason,
                "metrics": [exit_reason],
                "strategy": "short",
                "cooldown_count": 0,
                "signal_price": current_price,
            }

        return _hold_decision(status, latest_4h_time, score, exit_reason)

    return _hold_decision(status, latest_4h_time, score, metric_text)


def _hold_decision(status: dict[str, Any], bar_time: str, score: float, reason: str) -> dict[str, Any]:
    return {
        "action": "HOLD",
        "score": float(score if score is not None else status.get("last_score", 0.0)),
        "bar_time": bar_time,
        "reason": reason,
        "metrics": [reason],
        "strategy": "short",
        "cooldown_count": int(status.get("cooldown_count", 0) or 0),
    }


def _read_exit_action(exit_signal: Any) -> str:
    if exit_signal is None:
        return "HOLD"
    strategy_result = getattr(exit_signal, "StrategyResult", None)
    return str(getattr(strategy_result, "value", "HOLD"))


def _read_exit_reason(exit_signal: Any) -> str:
    if exit_signal is None:
        return "no_exit_signal"
    return str(getattr(exit_signal, "metric", "no_exit_signal"))


def _normalize_reason(reason: Any) -> str:
    if reason is None:
        return "no_metric"
    if isinstance(reason, list):
        return " | ".join(str(item) for item in reason) if reason else "no_metric"
    return str(reason)
