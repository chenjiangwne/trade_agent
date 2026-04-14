from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger

from strategy.FourHour_short import Res, eval_exit, testsuite_result


def make_decision(
    config: dict[str, Any],
    status: dict[str, Any],
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_daily: pd.DataFrame,
    latest_bar_time: str,
) -> dict[str, Any]:
    buypoint = config["basic"]["buypoint"]
    platform = config["basic"]["platform"]
    symbol = config["basic"]["symbol"]
    logger.info("--- Initializing [{}] strategy environment ---", platform)
    _log_scoring_inputs(df_1h, df_4h, df_daily, latest_bar_time)

    if status["position_status"] == "flat":
        logger.info("Initialization strategy environment! Fetching to {}", symbol)
        result, score, metrics = testsuite_result(df_1h, df_4h, df_daily)
        action = "SHORT" if result == Res["OK"] and score >= buypoint else "HOLD"
        metric_text = _normalize_reason(metrics)
        if result == Res["OK"] and score >= buypoint:
            logger.success(
                "--- OK! score={} >= buypoint={}, Execute short entry. metrics={} ---",
                score,
                buypoint,
                metric_text,
            )
        elif result == Res["OK"]:
            logger.warning(
                "--- OK! total score={} < buypoint={}, hold this round. metrics={} ---",
                score,
                buypoint,
                metric_text,
            )
        else:
            logger.error(
                "--- NOK! strategy scoring failed, result={}, score={}, buypoint={}, metrics={} ---",
                result,
                score,
                buypoint,
                metric_text,
            )
        return {
            "action": action,
            "score": float(score),
            "bar_time": latest_bar_time,
            "reason": metric_text,
            "metrics": metrics if isinstance(metrics, list) else [metric_text],
        }

    freeze_bars = int(config.get("trade", {}).get("exit_freeze_bars", 0))
    bars_since_entry = _bars_since_entry(df_1h, status.get("entry_time", ""))
    if freeze_bars > 0 and bars_since_entry <= freeze_bars:
        logger.warning(
            "--- FREEZE! bars_since_entry={} <= exit_freeze_bars={}, skip exit logic this round ---",
            bars_since_entry,
            freeze_bars,
        )
        return {
            "action": "HOLD",
            "score": float(status.get("last_score", 0.0)),
            "bar_time": latest_bar_time,
            "reason": f"exit frozen for first {freeze_bars} bars after entry",
            "metrics": [f"freeze_bars={freeze_bars}", f"bars_since_entry={bars_since_entry}"],
        }

    result, exit_signal = eval_exit(df_1h, df_4h, float(status["entry_price"]))
    exit_action = _read_exit_action(exit_signal)
    exit_reason = _read_exit_reason(exit_signal)
    should_exit = result == Res["OK"] and exit_action == "EXIT"
    return {
        "action": "EXIT" if should_exit else "HOLD",
        "score": float(status.get("last_score", 0.0)),
        "bar_time": latest_bar_time,
        "reason": exit_reason,
        "metrics": [exit_reason],
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


def _normalize_reason(reason: Any) -> str:
    if reason is None:
        return "no_metric"
    if isinstance(reason, list):
        return " | ".join(str(item) for item in reason) if reason else "no_metric"
    return str(reason)


def _log_scoring_inputs(df_1h: pd.DataFrame, df_4h: pd.DataFrame, df_daily: pd.DataFrame, latest_bar_time: str) -> None:
    # logger.debug(
    #     "scoring input summary: latest_4h_bar={} 4h_rows={} daily_rows={} 4h_start={} 4h_end={} daily_start={} daily_end={}",
    #     latest_bar_time,
    #     len(df_4h),
    #     len(df_daily),
    #     _safe_timestamp(df_4h, 0),
    #     _safe_timestamp(df_4h, -1),
    #     _safe_timestamp(df_daily, 0),
    #     _safe_timestamp(df_daily, -1),
    # )
    logger.debug("scoring input 1h tail={}", _tail_records(df_1h, 8))
    logger.debug("scoring input 4h tail={}", _tail_records(df_4h, 6))
    logger.debug("scoring input daily tail={}", _tail_records(df_daily, 6))


def _safe_timestamp(df: pd.DataFrame, index: int) -> str:
    if df.empty:
        return ""
    return str(pd.to_datetime(df.iloc[index]["timestamp"]).isoformat())


def _tail_records(df: pd.DataFrame, rows: int) -> str:
    if df.empty:
        return "[]"
    columns = [column for column in ["timestamp", "open", "high", "low", "close", "volume"] if column in df.columns]
    preview = df[columns].tail(rows).copy()
    preview["timestamp"] = pd.to_datetime(preview["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    return preview.to_json(orient="records", force_ascii=False)


def _bars_since_entry(df_4h: pd.DataFrame, entry_time: str) -> int:
    if df_4h.empty or not entry_time:
        return 0
    entry_ts = pd.to_datetime(entry_time)
    return int((pd.to_datetime(df_4h["timestamp"]) > entry_ts).sum())
