from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger

from generic.Common import (
    calc_atr,
    find_recent_swing_high,
    find_recent_swing_low,
    get_directional_basic_value,
)
from strategy import FourHour_long as long_strategy
from strategy import FourHour_short as short_strategy


def make_decision(
    config: dict[str, Any],
    status: dict[str, Any],
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_daily: pd.DataFrame,
    latest_bar_time: str,
    side: str = "short",
) -> dict[str, Any]:
    if side not in {"short", "long"}:
        raise ValueError(f"unsupported decision side: {side}")

    strategy = _strategy_api(side)
    buypoint = float(get_directional_basic_value(config, "buypoint", side, 0))
    buypoint_step = float(get_directional_basic_value(config, "buypoint_step", side, 3))
    entry_action = "SHORT" if side == "short" else "LONG"
    add_action = "ADD_SHORT" if side == "short" else "ADD_LONG"
    title = _log_title(side)

    platform = config["basic"]["platform"]
    symbol = config["basic"]["symbol"]
    _log_info(title, "--- Initializing [{}] strategy environment ---", platform)
    _log_scoring_inputs(df_1h, df_4h, df_daily, latest_bar_time, side)

    if status["position_status"] == "flat":
        _log_info(title, "strategy scoring {}", symbol)
        result, score, parameters = strategy["testsuite_result"](df_1h, df_4h, df_daily, log_title=title)
        metrics = parameters.get("metric_str", "") if isinstance(parameters, dict) else parameters
        parameters_log = parameters if isinstance(parameters, dict) else {"metric_str": parameters}
        metric_text = _normalize_reason(metrics)
        entry_price = float(df_4h.iloc[-1]["close"]) if not df_4h.empty else 0.0
        action = entry_action if result == strategy["Res"]["OK"] and score >= buypoint else "HOLD"
        initial_stop_price = strategy["compute_initial_stop"](entry_price, df_1h, df_4h) if action == entry_action else 0.0

        if result == strategy["Res"]["OK"] and score >= buypoint:
            _log_success(
                title,
                "--- OK! score={} >= buypoint={}, Execute {} entry. initial_stop_price={:.2f}, parameters={}, metric_str={} ---",
                score,
                buypoint,
                side,
                initial_stop_price,
                parameters_log,
                metric_text,
            )
        elif result == strategy["Res"]["OK"]:
            _log_warning(
                title,
                "--- OK! total score={} < buypoint={}, hold this round. parameters={}, metric_str={} ---",
                score,
                buypoint,
                parameters_log,
                metric_text,
            )
        else:
            _log_error(
                title,
                "--- NOK! strategy scoring failed, result={}, score={}, buypoint={}, parameters={}, metric_str={} ---",
                result,
                score,
                buypoint,
                parameters_log,
                metric_text,
            )

        return {
            "side": side,
            "action": action,
            "score": float(score),
            "entry_score": float(score),
            "bar_time": latest_bar_time,
            "reason": metric_text,
            "metrics": metrics if isinstance(metrics, list) else [metric_text],
            "status_updates": {
                "initial_entry_price": entry_price if action == entry_action else 0.0,
                "initial_stop_price": initial_stop_price,
                "peak_rr": 0.0,
            },
        }

    position_state = _build_position_state(status, df_1h, df_4h, side)
    result, score, parameters = strategy["testsuite_result"](df_1h, df_4h, df_daily, log_title=title)
    metrics = parameters.get("metric_str", "") if isinstance(parameters, dict) else parameters
    parameters_log = parameters if isinstance(parameters, dict) else {"metric_str": parameters}
    metric_text = _normalize_reason(metrics)
    if result == strategy["Res"]["OK"]:
        last_entry_score = float(status.get("last_entry_score", status.get("last_score", buypoint)))
        required_add_score = last_entry_score + buypoint_step
        if float(score) >= required_add_score:
            _log_success(
                title,
                "--- OK! add enabled: score={} >= required_add_score={} (last_entry_score={} + step={}). parameters={}, metric_str={} ---",
                score,
                required_add_score,
                last_entry_score,
                buypoint_step,
                parameters_log,
                metric_text,
            )
            return {
                "side": side,
                "action": add_action,
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
    initial_entry_price = position_state["initial_entry_price"]
    initial_stop_price = position_state["initial_stop_price"]
    peak_rr = position_state["peak_rr"]

    hard_stop_hit, hard_stop_price, hard_stop_reason = _check_hard_stop(side, df_4h.iloc[-1], initial_stop_price)
    if hard_stop_hit:
        _log_warning(title, "--- HARD STOP! {} ---", hard_stop_reason)
        return {
            "side": side,
            "action": "EXIT",
            "score": float(status.get("last_score", 0.0)),
            "bar_time": latest_bar_time,
            "reason": hard_stop_reason,
            "metrics": [hard_stop_reason],
            "exit_price": float(hard_stop_price),
            "status_updates": {
                "initial_entry_price": initial_entry_price,
                "initial_stop_price": initial_stop_price,
                "peak_rr": peak_rr,
            },
        }

    performance = strategy["calc_performance"](
        entry_price=initial_entry_price,
        current_price=current_close,
        stop_loss_price=initial_stop_price,
    )
    current_rr = performance["rr"]
    current_return_pct = performance["return_pct"]
    if current_rr is None:
        current_rr = 0.0
    peak_rr = max(float(peak_rr), float(current_rr))

    result, exit_signal = strategy["eval_exit"](
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
    should_exit = result == strategy["Res"]["OK"] and exit_action == "EXIT"
    return {
        "side": side,
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


def _strategy_api(side: str) -> dict[str, Any]:
    if side == "long":
        return {
            "Res": long_strategy.Res,
            "testsuite_result": long_strategy.testsuite_result,
            "eval_exit": long_strategy.eval_exit,
            "calc_performance": long_strategy.calc_long_performance,
            "compute_initial_stop": _compute_initial_long_stop,
        }
    return {
        "Res": short_strategy.Res,
        "testsuite_result": short_strategy.testsuite_result,
        "eval_exit": short_strategy.eval_exit,
        "calc_performance": short_strategy.calc_short_performance,
        "compute_initial_stop": _compute_initial_short_stop,
    }


def _build_position_state(status: dict[str, Any], df_1h: pd.DataFrame, df_4h: pd.DataFrame, side: str) -> dict[str, float]:
    initial_entry_price = float(status.get("initial_entry_price", status.get("entry_price", 0.0)) or 0.0)
    initial_stop_price = float(status.get("initial_stop_price", 0.0) or 0.0)
    peak_rr = float(status.get("peak_rr", 0.0) or 0.0)

    if initial_entry_price <= 0:
        initial_entry_price = float(status.get("entry_price", 0.0) or 0.0)

    if initial_entry_price > 0 and initial_stop_price <= 0:
        initial_stop_price = _strategy_api(side)["compute_initial_stop"](initial_entry_price, df_1h, df_4h)
        _log_warning(
            _log_title(side),
            "--- rebuilt missing risk state from persisted entry: entry_price={:.2f}, initial_stop_price={:.2f} ---",
            initial_entry_price,
            initial_stop_price,
        )

    return {
        "initial_entry_price": float(initial_entry_price),
        "initial_stop_price": float(initial_stop_price),
        "peak_rr": float(peak_rr),
    }


def _compute_initial_short_stop(entry_price: float, df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> float:
    atr_4h = calc_atr(df_4h, period=14).iloc[-1] if not df_4h.empty else 0.0
    if pd.isna(atr_4h):
        atr_4h = 0.0
    recent_high = find_recent_swing_high(df_1h, lookback=20)
    struct_stop = recent_high + 0.3 * atr_4h if not pd.isna(recent_high) else 0.0
    vol_stop = entry_price + 1.5 * atr_4h
    return float(max(struct_stop, vol_stop, entry_price * 1.01))


def _compute_initial_long_stop(entry_price: float, df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> float:
    atr_4h = calc_atr(df_4h, period=14).iloc[-1] if not df_4h.empty else 0.0
    if pd.isna(atr_4h):
        atr_4h = 0.0
    recent_low = find_recent_swing_low(df_1h, lookback=20)
    struct_stop = recent_low - 0.3 * atr_4h if not pd.isna(recent_low) else entry_price * 0.99
    vol_stop = entry_price - 1.5 * atr_4h
    return float(max(struct_stop, vol_stop, entry_price * 0.99))


def _check_hard_stop(side: str, current_k: pd.Series, initial_stop_price: float) -> tuple[bool, float, str]:
    if initial_stop_price <= 0:
        return False, 0.0, ""
    if side == "short":
        current_high = float(current_k["high"])
        if current_high >= initial_stop_price:
            return True, float(initial_stop_price), f"short hard stop: high={current_high}, stop={initial_stop_price}"
    else:
        current_low = float(current_k["low"])
        if current_low <= initial_stop_price:
            return True, float(initial_stop_price), f"long hard stop: low={current_low}, stop={initial_stop_price}"
    return False, 0.0, ""


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


def _log_scoring_inputs(
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_daily: pd.DataFrame,
    latest_bar_time: str,
    side: str,
) -> None:
    title = _log_title(side)
    _log_debug(title, "scoring input latest_bar_time={}", latest_bar_time)
    _log_debug(title, "scoring input 1h tail={}", _tail_records(df_1h, 8))
    _log_debug(title, "scoring input 4h tail={}", _tail_records(df_4h, 6))
    _log_debug(title, "scoring input daily tail={}", _tail_records(df_daily, 6))


def _log_title(side: str) -> str:
    return "<green>[LONG]</green>" if side == "long" else "<red>[SHORT]</red>"


def _log_debug(title: str, message: str, *args: Any) -> None:
    logger.opt(colors=True).debug(f"{title} {message}", *args)


def _log_info(title: str, message: str, *args: Any) -> None:
    logger.opt(colors=True).info(f"{title} {message}", *args)


def _log_success(title: str, message: str, *args: Any) -> None:
    logger.opt(colors=True).success(f"{title} {message}", *args)


def _log_warning(title: str, message: str, *args: Any) -> None:
    logger.opt(colors=True).warning(f"{title} {message}", *args)


def _log_error(title: str, message: str, *args: Any) -> None:
    logger.opt(colors=True).error(f"{title} {message}", *args)


def _tail_records(df: pd.DataFrame, rows: int) -> str:
    if df.empty:
        return "[]"
    columns = [column for column in ["timestamp", "open", "high", "low", "close", "volume"] if column in df.columns]
    preview = df[columns].tail(rows).copy()
    preview["timestamp"] = pd.to_datetime(preview["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    return preview.to_json(orient="records", force_ascii=False)
