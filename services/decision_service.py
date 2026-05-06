from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger

from strategy.FourHour_short import Res, _calc_atr, _find_recent_swing_high, calc_short_performance, eval_exit, testsuite_result


def make_decision(
    config: dict[str, Any],
    status: dict[str, Any],
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_daily: pd.DataFrame,
    latest_bar_time: str,
) -> dict[str, Any]:
    buypoint = config["basic"]["buypoint"]
    buypoint_step = float(config.get("basic", {}).get("buypoint_step", 3))
    platform = config["basic"]["platform"]
    symbol = config["basic"]["symbol"]
    logger.info("--- Initializing [{}] strategy environment ---", platform)
    _log_scoring_inputs(df_1h, df_4h, df_daily, latest_bar_time)

    if status["position_status"] == "flat":
        logger.info("Initialization strategy environment! Fetching to {}", symbol)
        result, score, parameters = testsuite_result(df_1h, df_4h, df_daily)
        metrics = parameters.get("metric_str", "") if isinstance(parameters, dict) else parameters
        parameters_log = parameters if isinstance(parameters, dict) else {"metric_str": parameters}
        action = "SHORT" if result == Res["OK"] and score >= buypoint else "HOLD"
        metric_text = _normalize_reason(metrics)
        entry_price = float(df_4h.iloc[-1]["close"]) if not df_4h.empty else 0.0
        initial_stop_price = 0.0
        if action == "SHORT":
            initial_stop_price = _compute_initial_short_stop(entry_price, df_1h, df_4h)

        if result == Res["OK"] and score >= buypoint:
            logger.success(
                "--- OK! score={} >= buypoint={}, Execute short entry. initial_stop_price={:.2f}, parameters={}, metric_str={} ---",
                score,
                buypoint,
                initial_stop_price,
                parameters_log,
                metric_text,
            )
        elif result == Res["OK"]:
            logger.warning(
                "--- OK! total score={} < buypoint={}, hold this round. parameters={}, metric_str={} ---",
                score,
                buypoint,
                parameters_log,
                metric_text,
            )
        else:
            logger.error(
                "--- NOK! strategy scoring failed, result={}, score={}, buypoint={}, parameters={}, metric_str={} ---",
                result,
                score,
                buypoint,
                parameters_log,
                metric_text,
            )
        return {
            "action": action,
            "score": float(score),
            "entry_score": float(score),
            "bar_time": latest_bar_time,
            "reason": metric_text,
            "metrics": metrics if isinstance(metrics, list) else [metric_text],
            "status_updates": {
                "initial_entry_price": entry_price if action == "SHORT" else 0.0,
                "initial_stop_price": initial_stop_price,
                "peak_rr": 0.0,
            },
        }

    position_state = _build_position_state(status, df_1h, df_4h)

    result, score, parameters = testsuite_result(df_1h, df_4h, df_daily)
    metrics = parameters.get("metric_str", "") if isinstance(parameters, dict) else parameters
    parameters_log = parameters if isinstance(parameters, dict) else {"metric_str": parameters}
    metric_text = _normalize_reason(metrics)
    if result == Res["OK"]:
        last_entry_score = float(status.get("last_entry_score", status.get("last_score", buypoint)))
        required_add_score = last_entry_score + buypoint_step
        if float(score) >= required_add_score:
            logger.success(
                "--- OK! add short enabled: score={} >= required_add_score={} (last_entry_score={} + step={}). parameters={}, metric_str={} ---",
                score,
                required_add_score,
                last_entry_score,
                buypoint_step,
                parameters_log,
                metric_text,
            )
            return {
                "action": "ADD_SHORT",
                "score": float(score),
                "entry_score": float(score),
                "bar_time": latest_bar_time,
                "reason": metric_text,
                "metrics": [metric_text] if metric_text else [],
                "status_updates": {
                    "initial_entry_price": position_state["initial_entry_price"],
                    "initial_stop_price": position_state["initial_stop_price"],
                    "peak_rr": position_state["peak_rr"],
                },
            }

    current_close = float(df_4h.iloc[-1]["close"])
    current_high = float(df_4h.iloc[-1]["high"])
    initial_entry_price = position_state["initial_entry_price"]
    initial_stop_price = position_state["initial_stop_price"]
    peak_rr = position_state["peak_rr"]

    if initial_stop_price > 0 and current_high >= initial_stop_price:
        exit_reason = f"硬止损触发：最高价 {current_high} 触及挂单位 {initial_stop_price}"
        logger.warning("--- HARD STOP! {} ---", exit_reason)
        return {
            "action": "EXIT",
            "score": float(status.get("last_score", 0.0)),
            "bar_time": latest_bar_time,
            "reason": exit_reason,
            "metrics": [exit_reason],
            "exit_price": float(initial_stop_price),
            "status_updates": {
                "initial_entry_price": initial_entry_price,
                "initial_stop_price": initial_stop_price,
                "peak_rr": peak_rr,
            },
        }

    performance = calc_short_performance(
        entry_price=initial_entry_price,
        current_price=current_close,
        stop_loss_price=initial_stop_price,
    )
    current_rr = performance["rr"]
    current_return_pct = performance["return_pct"]
    if current_rr is None:
        current_rr = 0.0
    peak_rr = max(float(peak_rr), float(current_rr))

    result, exit_signal = eval_exit(
        df_1h=df_1h,
        df_4h=df_4h,
        current_price=current_close,
        initial_stop=initial_stop_price,
        current_rr=current_rr,
        peak_rr=peak_rr,
        return_pct=current_return_pct,
    )
    exit_action = _read_exit_action(exit_signal)
    exit_reason = _read_exit_reason(exit_signal)
    should_exit = result == Res["OK"] and exit_action == "EXIT"
    return {
        "action": "EXIT" if should_exit else "HOLD",
        "score": float(status.get("last_score", 0.0)),
        "bar_time": latest_bar_time,
        "reason": exit_reason,
        "metrics": [exit_reason],
        "exit_price": current_close,
        "status_updates": {
            "initial_entry_price": initial_entry_price,
            "initial_stop_price": initial_stop_price,
            "peak_rr": peak_rr,
        },
    }


def _build_position_state(status: dict[str, Any], df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> dict[str, float]:
    initial_entry_price = float(status.get("initial_entry_price", status.get("entry_price", 0.0)) or 0.0)
    initial_stop_price = float(status.get("initial_stop_price", 0.0) or 0.0)
    peak_rr = float(status.get("peak_rr", 0.0) or 0.0)

    if initial_entry_price <= 0:
        initial_entry_price = float(status.get("entry_price", 0.0) or 0.0)

    if initial_entry_price > 0 and initial_stop_price <= 0:
        rebuilt = _rebuild_initial_short_state(status, df_1h, df_4h, initial_entry_price)
        initial_entry_price = rebuilt["initial_entry_price"]
        initial_stop_price = rebuilt["initial_stop_price"]
        logger.warning(
            "--- rebuilt missing short risk state from persisted entry: entry_price={:.2f}, initial_stop_price={:.2f} ---",
            initial_entry_price,
            initial_stop_price,
        )

    return {
        "initial_entry_price": float(initial_entry_price),
        "initial_stop_price": float(initial_stop_price),
        "peak_rr": float(peak_rr),
    }


def _rebuild_initial_short_state(
    status: dict[str, Any],
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    entry_price: float,
) -> dict[str, float]:
    entry_time = status.get("entry_time", "")
    if entry_time:
        entry_ts = pd.to_datetime(entry_time)
        entry_df_1h = df_1h[pd.to_datetime(df_1h["timestamp"]) <= entry_ts].copy()
        entry_df_4h = df_4h[pd.to_datetime(df_4h["timestamp"]) <= entry_ts].copy()
        if not entry_df_1h.empty and not entry_df_4h.empty:
            return {
                "initial_entry_price": float(entry_price),
                "initial_stop_price": _compute_initial_short_stop(entry_price, entry_df_1h, entry_df_4h),
            }

    return {
        "initial_entry_price": float(entry_price),
        "initial_stop_price": _compute_initial_short_stop(entry_price, df_1h, df_4h),
    }


def _compute_initial_short_stop(entry_price: float, df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> float:
    atr_4h = _calc_atr(df_4h, period=14).iloc[-1] if not df_4h.empty else 0.0
    if pd.isna(atr_4h):
        atr_4h = 0.0
    recent_high = _find_recent_swing_high(df_1h, lookback=20)
    struct_stop = recent_high + 0.3 * atr_4h if not pd.isna(recent_high) else 0.0
    vol_stop = entry_price + 1.5 * atr_4h
    return float(max(struct_stop, vol_stop, entry_price * 1.01))


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
    logger.debug("scoring input 1h tail={}", _tail_records(df_1h, 8))
    logger.debug("scoring input 4h tail={}", _tail_records(df_4h, 6))
    logger.debug("scoring input daily tail={}", _tail_records(df_daily, 6))


def _tail_records(df: pd.DataFrame, rows: int) -> str:
    if df.empty:
        return "[]"
    columns = [column for column in ["timestamp", "open", "high", "low", "close", "volume"] if column in df.columns]
    preview = df[columns].tail(rows).copy()
    preview["timestamp"] = pd.to_datetime(preview["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    return preview.to_json(orient="records", force_ascii=False)


def _bars_since_entry(df_1h: pd.DataFrame, entry_time: str) -> int | None:
    if df_1h.empty or not entry_time:
        return None
    entry_ts = pd.to_datetime(entry_time, errors="coerce")
    if pd.isna(entry_ts):
        return None
    return int((pd.to_datetime(df_1h["timestamp"]) > entry_ts).sum())
