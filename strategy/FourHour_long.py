from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger


Res = {"OK": 0, "ERR": -1, "EXCEPTION": -2}


class StrategyResult(Enum):
    WAIT = "WAIT"
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT = "EXIT"
    ERROR = "ERROR"


@dataclass
class Monitor:
    StrategyResult: StrategyResult
    metric: str | list[str]
    score: float
    timestamp: int
    fallback_value: float = 0.0


def _now_ts() -> int:
    return int(time.time() * 1000)


def _calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = pd.concat(
        [
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def _calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))


def _find_recent_swing_low(df: pd.DataFrame, lookback: int = 10) -> float:
    if df is None or len(df) < lookback:
        return np.nan
    return float(df["low"].iloc[-lookback:].min())


def _range_position(df: pd.DataFrame, lookback: int = 40) -> float:
    if df is None or len(df) < lookback:
        return np.nan
    recent = df.iloc[-lookback:]
    low = recent["low"].min()
    high = recent["high"].max()
    close = recent["close"].iloc[-1]
    if pd.isna(low) or pd.isna(high) or high <= low:
        return np.nan
    return float((close - low) / (high - low + 1e-9))


def _find_local_swing_points(df: pd.DataFrame, left: int = 2, right: int = 2) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    if df is None or len(df) < left + right + 5:
        return [], []
    highs: list[tuple[int, float]] = []
    lows: list[tuple[int, float]] = []
    h = df["high"].values
    l = df["low"].values
    for i in range(left, len(df) - right):
        if h[i] == max(h[i - left : i + right + 1]):
            highs.append((i, float(h[i])))
        if l[i] == min(l[i - left : i + right + 1]):
            lows.append((i, float(l[i])))
    return highs, lows


def is_system_ready(df_4h: pd.DataFrame, df_daily: pd.DataFrame, allow_stale: bool = True) -> bool:
    if df_4h is None or df_daily is None or len(df_4h) < 200 or len(df_daily) < 40:
        logger.error("NOK! long strategy data deficiency")
        return False
    return True


def eval_trend(df_4h: pd.DataFrame, df_daily: pd.DataFrame) -> tuple[int, Monitor | None]:
    if not is_system_ready(df_4h, df_daily, allow_stale=True):
        return Res["ERR"], None

    d = df_daily.copy()
    h4 = df_4h.copy()
    d["ema200"] = d["close"].ewm(span=200, adjust=False).mean()
    h4["ema50"] = h4["close"].ewm(span=50, adjust=False).mean()
    h4["ema200"] = h4["close"].ewm(span=200, adjust=False).mean()

    last_d = d.iloc[-1]
    prev_d = d.iloc[-2]
    last_4h = h4.iloc[-1]
    prev_4h = h4.iloc[-2]

    score = 0
    metrics: list[str] = []

    ema200_slope = last_d["ema200"] - prev_d["ema200"]
    if last_d["close"] > last_d["ema200"] * 1.01:
        if ema200_slope > 0:
            score += 5
            metrics.append("daily above rising EMA200 (+5)")
        else:
            score += 2
            metrics.append("daily above flat EMA200 (+2)")
    else:
        metrics.append("daily trend filter failed (+0)")

    if last_4h["ema50"] > last_4h["ema200"]:
        score += 2
        metrics.append("4h EMA50 > EMA200 (+2)")
    if last_4h["close"] > last_4h["ema50"]:
        score += 1
        metrics.append("4h close above EMA50 (+1)")
    if (last_4h["ema50"] - prev_4h["ema50"]) > 0:
        score += 1
        metrics.append("4h EMA50 rising (+1)")

    return Res["OK"], Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=" | ".join(metrics) if metrics else "trend neutral",
        score=score,
        timestamp=_now_ts(),
    )


def eval_position(df_4h: pd.DataFrame) -> tuple[int, Monitor | None]:
    if df_4h is None or len(df_4h) < 30:
        return Res["ERR"], None

    h4 = df_4h.copy()
    h4["bb_mid"] = h4["close"].rolling(20).mean()
    h4["bb_std"] = h4["close"].rolling(20).std()
    h4["bb_lower"] = h4["bb_mid"] - 2 * h4["bb_std"]
    h4["ema200"] = h4["close"].ewm(span=200, adjust=False).mean()
    h4["zscore"] = (h4["close"] - h4["bb_mid"]) / (h4["bb_std"] + 1e-9)

    last = h4.iloc[-1]
    prev = h4.iloc[-2]
    z = float(last["zscore"])
    score = 0
    metrics: list[str] = []

    if last["close"] < last["ema200"]:
        score -= 2
        metrics.append("below EMA200 penalty (-2)")

    if -2.5 < z <= -1:
        score += 1
        metrics.append(f"pullback zone z={z:.2f} (+1)")
    elif -3 < z <= -2.5:
        score += 2
        metrics.append(f"deep pullback z={z:.2f} (+2)")

    if prev["close"] < prev["bb_lower"] and last["close"] > last["bb_lower"]:
        score += 3
        metrics.append("reclaim Bollinger lower band (+3)")

    return Res["OK"], Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=" | ".join(metrics) if metrics else "position neutral",
        score=score,
        timestamp=_now_ts(),
    )


def eval_long_trigger_1h(df_1h: pd.DataFrame) -> tuple[int, Monitor | None]:
    if df_1h is None or len(df_1h) < 100:
        return Res["ERR"], None

    close = df_1h["close"]
    high = df_1h["high"]
    score = 0
    metrics: list[str] = []
    ema15 = close.ewm(span=15, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()

    if ema15.iloc[-1] > ema50.iloc[-1]:
        score += 1
        metrics.append("1h EMA15 > EMA50 (+1)")
    if close.iloc[-2] < ema15.iloc[-2] and close.iloc[-1] > ema15.iloc[-1]:
        score += 2
        metrics.append("1h reclaim EMA15 (+2)")
    if (ema15.iloc[-2] - ema15.iloc[-3]) < 0 and (ema15.iloc[-1] - ema15.iloc[-2]) > 0:
        score += 2
        metrics.append("1h EMA15 turns up (+2)")
    if close.iloc[-1] > high.iloc[-6:-1].max():
        score += 2
        metrics.append("1h breakout recent high (+2)")

    vol_ma = df_1h["volume"].rolling(20).mean() if "volume" in df_1h.columns else None
    if vol_ma is not None and df_1h["volume"].iloc[-1] > vol_ma.iloc[-1] * 1.5:
        score += 1
        metrics.append("1h volume expansion (+1)")

    action = StrategyResult.LONG if score >= 4 else StrategyResult.WAIT
    return Res["OK"], Monitor(
        StrategyResult=action,
        metric=" | ".join(metrics) if metrics else "1h trigger neutral",
        score=score,
        timestamp=_now_ts(),
    )


def eval_regime(df_4h: pd.DataFrame) -> tuple[int, Monitor | None]:
    if df_4h is None or len(df_4h) < 80:
        return Res["ERR"], None

    h4 = df_4h.copy()
    close = h4["close"]
    high = h4["high"]
    low = h4["low"]
    atr = _calc_atr(h4, 14)
    atr_mean = atr.rolling(50).mean()
    atr_ratio = atr.iloc[-1] / (atr_mean.iloc[-1] + 1e-9)
    ema15 = close.ewm(span=15, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()

    score = 0
    metrics: list[str] = []
    if atr_ratio > 1.2:
        score += 2
        metrics.append("ATR expanding (+2)")
    elif atr_ratio > 1.0:
        score += 1
        metrics.append("ATR normal (+1)")

    plus_dm = high.diff().clip(lower=0)
    minus_dm = (-low.diff()).clip(lower=0)
    tr_smooth = _calc_atr(h4, 14).ewm(alpha=1 / 14, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / 14, adjust=False).mean() / (tr_smooth + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / 14, adjust=False).mean() / (tr_smooth + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    adx = dx.ewm(alpha=1 / 14, adjust=False).mean().iloc[-1]
    if adx > 25:
        score += 2
        metrics.append("strong trend regime (+2)")
    elif adx > 20:
        score += 1
        metrics.append("weak trend regime (+1)")

    trend_count = (close > ema50).rolling(20).sum().iloc[-1]
    if trend_count > 15:
        score -= 2
        metrics.append("extended trend penalty (-2)")

    spread = abs(ema15.iloc[-1] - ema50.iloc[-1]) / (ema50.iloc[-1] + 1e-9)
    if spread > 0.01:
        score += 1
        metrics.append("EMA spread expanded (+1)")

    return Res["OK"], Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=" | ".join(metrics) if metrics else "regime neutral",
        score=score,
        timestamp=_now_ts(),
    )


def eval_momentum(df_4h: pd.DataFrame) -> tuple[int, Monitor | None]:
    if df_4h is None or len(df_4h) < 60:
        return Res["ERR"], None

    close = df_4h["close"]
    ema15 = close.ewm(span=15, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    score = 0
    metrics: list[str] = []

    if ema15.iloc[-1] > ema50.iloc[-1]:
        score += 1
        metrics.append("4h EMA15 > EMA50 (+1)")
    if ema15.iloc[-1] > ema15.iloc[-2]:
        score += 1
        metrics.append("4h EMA15 rising (+1)")
    if close.iloc[-1] > ema15.iloc[-1]:
        score += 1
        metrics.append("4h close above EMA15 (+1)")

    return Res["OK"], Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=" | ".join(metrics) if metrics else "momentum neutral",
        score=score,
        timestamp=_now_ts(),
    )


def eval_rsi(df_4h: pd.DataFrame) -> tuple[int, Monitor | None]:
    if df_4h is None or len(df_4h) < 20:
        return Res["ERR"], None

    rsi = _calc_rsi(df_4h["close"], 14)
    rsi_t = rsi.iloc[-1]
    rsi_t1 = rsi.iloc[-2]
    rsi_t2 = rsi.iloc[-3]
    oversold = rsi_t < 40
    rebound = rsi_t > rsi_t1
    turning_point = rsi_t1 < rsi_t2 and rsi_t > rsi_t1
    if oversold and rebound and turning_point:
        return Res["OK"], Monitor(
            StrategyResult=StrategyResult.WAIT,
            metric=f"RSI oversold rebound rsi={rsi_t:.2f} (+1)",
            score=1,
            timestamp=_now_ts(),
        )
    return Res["OK"], Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=f"RSI neutral rsi={rsi_t:.2f} (+0)",
        score=0,
        timestamp=_now_ts(),
    )


def eval_long_risk(df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> tuple[int, Monitor | None]:
    if df_1h is None or df_4h is None or len(df_1h) < 30 or len(df_4h) < 20:
        return Res["ERR"], None

    h4 = df_4h.copy()
    atr_last = _calc_atr(h4, period=14).iloc[-1]
    current_price = float(df_1h["close"].iloc[-1])
    swing_low = _find_recent_swing_low(df_1h, lookback=10)
    if pd.isna(swing_low) or pd.isna(atr_last):
        return Res["ERR"], None

    stop_loss = float(swing_low - 0.3 * atr_last)
    risk_pct = (current_price - stop_loss) / (current_price + 1e-9)
    score = 0
    metrics: list[str] = []

    if stop_loss < current_price:
        score += 6
        metrics.append(f"structure stop valid stop={stop_loss:.4f} (+6)")
    if 0 < risk_pct <= 0.01:
        score += 5
        metrics.append("risk distance <=1% (+5)")
    elif 0.01 < risk_pct <= 0.02:
        score += 4
        metrics.append("risk distance <=2% (+4)")
    elif 0.02 < risk_pct <= 0.03:
        score += 2
        metrics.append("risk distance <=3% (+2)")
    else:
        metrics.append("risk distance too wide (+0)")

    ema20_1h = df_1h["close"].ewm(span=20, adjust=False).mean().iloc[-1]
    stretch = (current_price - ema20_1h) / (ema20_1h + 1e-9)
    if stretch < 0.012:
        score += 4
        metrics.append("not overextended above EMA20 (+4)")

    action = StrategyResult.LONG if score >= 10 else StrategyResult.WAIT
    return Res["OK"], Monitor(
        StrategyResult=action,
        metric=" | ".join(metrics),
        score=min(score, 15),
        timestamp=_now_ts(),
        fallback_value=float(round(stop_loss, 4)),
    )


def eval_exit(
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    current_price: float,
    initial_stop: float,
    current_rr: float,
    peak_rr: float,
    return_pct: float,
) -> tuple[int, Monitor | None]:
    try:
        ema20 = df_1h["close"].ewm(span=20, adjust=False).mean()
        atr = _calc_atr(df_4h, period=14).iloc[-1]
        action = StrategyResult.WAIT
        metric = "hold long"

        if return_pct <= -0.03:
            action = StrategyResult.EXIT
            metric = f"hard stop: return_pct={return_pct * 100:.2f}%"
            return Res["OK"], Monitor(action, metric, 0, _now_ts())

        if current_price <= initial_stop:
            action = StrategyResult.EXIT
            metric = f"structure stop broken: stop={initial_stop:.2f}"
            return Res["OK"], Monitor(action, metric, 0, _now_ts())

        if peak_rr >= 1.5:
            if peak_rr >= 3.0:
                atr_mult = 1.0
            elif peak_rr >= 2.0:
                atr_mult = 1.5
            else:
                atr_mult = 2.0
            recent_highest_high = df_1h["high"].rolling(12).max().iloc[-1]
            trailing_stop = recent_highest_high - (atr_mult * atr)
            if current_price <= trailing_stop and current_rr > 0.5:
                action = StrategyResult.EXIT
                metric = f"trailing stop: peak_rr={peak_rr:.2f}, stop={trailing_stop:.2f}"
                return Res["OK"], Monitor(action, metric, 0, _now_ts())

        if peak_rr < 1.5 and current_price < ema20.iloc[-1] and ema20.iloc[-1] < ema20.iloc[-2]:
            action = StrategyResult.EXIT
            metric = "trend broken: below falling EMA20"
            return Res["OK"], Monitor(action, metric, 0, _now_ts())

        return Res["OK"], Monitor(action, metric, 0, _now_ts())
    except Exception as exc:
        logger.error(f"eval_exit_long error: {exc}")
        return Res["ERR"], None


def testsuite_result(df_1h: pd.DataFrame, df_4h: pd.DataFrame, df_daily: pd.DataFrame) -> tuple[int, float, dict[str, Any]]:
    total_score = 0.0
    parameters: dict[str, Any] = {}
    metrics: list[str] = []
    risk_stop_price: list[str] = []
    try:
        checks = {
            "eval_regime": eval_regime(df_4h),
            "eval_trend": eval_trend(df_4h, df_daily),
            "eval_position": eval_position(df_4h),
            "eval_momentum": eval_momentum(df_4h),
            "eval_rsi": eval_rsi(df_4h),
            "eval_long_trigger_1h": eval_long_trigger_1h(df_1h),
            "eval_long_risk": eval_long_risk(df_1h, df_4h),
        }

        monitors: dict[str, Monitor] = {}
        for name, (result, monitor) in checks.items():
            logger.info(f"test_case:{name} -> execute Result is>> {result}, detail: {monitor}")
            if result != Res["OK"] or monitor is None:
                parameters.update({"failed_case": name, "metric_str": " | ".join(metrics)})
                return Res["ERR"], total_score, parameters
            monitors[name] = monitor

        regime = monitors["eval_regime"]
        position = monitors["eval_position"]
        ema50 = df_4h["close"].ewm(span=50, adjust=False).mean()
        distance = (df_4h["close"].iloc[-1] - ema50.iloc[-1]) / (ema50.iloc[-1] + 1e-9)

        parameters.update(
            {
                "regime_score": regime.score,
                "position_score": position.score,
                "ema50_distance": float(distance),
                "preconditions_met": regime.score >= 2 and distance <= 0.08,
            }
        )

        if regime.score < 2:
            parameters["metric_str"] = f"[eval_regime] {regime.metric}"
            return Res["OK"], 0.0, parameters
        if distance > 0.08:
            parameters["metric_str"] = f"price too far above EMA50: distance={distance:.2%}"
            return Res["OK"], 0.0, parameters

        for name, monitor in monitors.items():
            weight = 1.5 if name == "eval_long_trigger_1h" else 1.0
            total_score += float(monitor.score) * weight
            metrics.append(f"[{name}] {monitor.metric}")
            if monitor.fallback_value:
                risk_stop_price.append(f"[{name}] {monitor.fallback_value}")

        parameters["metric_str"] = " | ".join(metrics)
        parameters["risk_stop_price"] = risk_stop_price
        parameters["total_score"] = total_score
        return Res["OK"], total_score, parameters
    except Exception as exc:
        logger.error(f"NOK! testsuite_long_result err:{exc}")
        return Res["ERR"], total_score, parameters


def calc_long_performance(entry_price: float, current_price: float, stop_loss_price: float | None = None) -> dict[str, float | None]:
    if entry_price is None or entry_price <= 0:
        return {"return_pct": 0.0, "rr": None}
    return_pct = (current_price - entry_price) / entry_price
    rr = None
    if stop_loss_price is not None:
        initial_risk = entry_price - stop_loss_price
        if initial_risk > 0:
            rr = (current_price - entry_price) / initial_risk
        elif initial_risk < 0:
            rr = 0.0
    return {"return_pct": return_pct, "rr": rr}
