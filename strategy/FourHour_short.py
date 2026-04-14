from enum import Enum
from dataclasses import dataclass
import time
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
    metric: str
    score: int
    timestamp: int


curr_time = int(time.time() * 1000)


# =========================================================
# Helper
# =========================================================
def _now_ts():
    return int(time.time() * 1000)


def _safe_last(series, default=np.nan):
    if series is None or len(series) == 0:
        return default
    return series.iloc[-1]


def _calc_boll(df, window=20, std_mul=2):
    df = df.copy()
    df["bb_mid"] = df["close"].rolling(window=window).mean()
    df["bb_std"] = df["close"].rolling(window=window).std()
    df["bb_upper"] = df["bb_mid"] + std_mul * df["bb_std"]
    df["bb_lower"] = df["bb_mid"] - std_mul * df["bb_std"]
    return df


def _calc_macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_hist = dif - dea
    return dif, dea, macd_hist


def _calc_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / (avg_loss + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _calc_atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    atr = tr.rolling(period).mean()
    return atr


def _is_near_level(price, level, tolerance=0.01):
    """
    tolerance=0.01 -> 1%
    """
    if pd.isna(price) or pd.isna(level) or level == 0:
        return False
    return abs(price - level) / abs(level) <= tolerance


def _in_zone(price, low, high):
    if pd.isna(price) or pd.isna(low) or pd.isna(high):
        return False
    return low <= price <= high


def _calc_fib_retracement_zone(df_4h, lookback=60):
    """
    找最近一段4H swing low -> swing high 的回撤区
    用于判断是否反弹到 0.618 / 0.786 区
    """
    if df_4h is None or len(df_4h) < lookback:
        return None

    swing = df_4h.iloc[-lookback:]
    swing_high = swing["high"].max()
    swing_low = swing["low"].min()

    if pd.isna(swing_high) or pd.isna(swing_low) or swing_high <= swing_low:
        return None

    diff = swing_high - swing_low

    fib_618 = swing_low + diff * 0.618
    fib_786 = swing_low + diff * 0.786

    zone_low = min(fib_618, fib_786)
    zone_high = max(fib_618, fib_786)

    return {
        "swing_high": swing_high,
        "swing_low": swing_low,
        "fib_618": fib_618,
        "fib_786": fib_786,
        "zone_low": zone_low,
        "zone_high": zone_high
    }


def _find_recent_swing_high(df, lookback=10):
    """
    最近 lookback 根K线的最高点，作为结构止损参考
    """
    if df is None or len(df) < lookback:
        return np.nan
    return df["high"].iloc[-lookback:].max()


def _has_hh_hl_structure(df, lookback=20):
    """
    粗略判断是否已转成 HH + HL 牛市结构
    """
    if df is None or len(df) < lookback:
        return False

    recent = df.iloc[-lookback:]
    highs_1 = recent["high"].iloc[-10:].max()
    highs_0 = recent["high"].iloc[-20:-10].max() if len(recent) >= 20 else recent["high"].iloc[:-10].max()

    lows_1 = recent["low"].iloc[-10:].min()
    lows_0 = recent["low"].iloc[-20:-10].min() if len(recent) >= 20 else recent["low"].iloc[:-10].min()

    if pd.isna(highs_0) or pd.isna(highs_1) or pd.isna(lows_0) or pd.isna(lows_1):
        return False

    return highs_1 > highs_0 and lows_1 > lows_0


# 满分 25
# =========================================================
def eval_short_background(df_daily, df_4h):
    """
    Description:
    日线是否给空头背景
    核心:
    1. 日线仍在EMA200下方
    2. EMA200 slope 向下
    3. 近期未突破关键前高
    4. 尚未形成 HH + HL 牛市反转结构
    """

    res = Res["OK"]

    if df_daily is None or df_4h is None or len(df_daily) < 220 or len(df_4h) < 50:
        logger.error("NOK! Daily/4H data deficiency")
        return Res["ERR"], None

    try:
        d = df_daily.copy()

        d["ema50"] = d["close"].ewm(span=50, adjust=False).mean()
        d["ema200"] = d["close"].ewm(span=200, adjust=False).mean()

        last = d.iloc[-1]
        prev = d.iloc[-2]

        score = 0
        metrics = []

        # 1) close < ema200
        if last["close"] < last["ema200"]:
            score += 10
            metrics.append("日线收盘仍在EMA200下方 (+10)")
        else:
            metrics.append("日线已站上EMA200 (+0)")

        # 2) ema200 slope < 0
        ema200_slope = last["ema200"] - prev["ema200"]
        if ema200_slope < 0:
            score += 5
            metrics.append("日线EMA200下行 (+5)")
        else:
            metrics.append("日线EMA200未下行 (+0)")

        # 3) ema50 < ema200
        if last["ema50"] < last["ema200"]:
            score += 5
            metrics.append("日线EMA50 < EMA200 (+5)")
        else:
            metrics.append("日线EMA50 >= EMA200 (+0)")

        # 4) 尚未形成 HH+HL 牛市结构
        if not _has_hh_hl_structure(d, lookback=20):
            score += 5
            metrics.append("日线未形成HH+HL反转结构 (+5)")
        else:
            metrics.append("日线已出现HH+HL雏形 (+0)")

        action = StrategyResult.WAIT
        # if score >= 18:
        #     action = StrategyResult.SHORT

        return res, Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics),
            score=min(score, 25),
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_short_background error: {e}")
        return Res["ERR"], None


# =========================================================
# 2. 日线 / 4H 压力区评分
# 满分 35
# =========================================================
def eval_short_resistance_zone(df_daily, df_4h):
    """
    Description:
    日线/4H 是否到了明确压力区

    参考:
    1. 日线前高附近
    2. 日线BOLL上轨附近
    3. 4H前高附近
    4. 4H平台上沿
    5. 4H BOLL上轨附近
    6. 4H fib 0.618/0.786 回撤区
    """

    res = Res["OK"]

    if df_daily is None or df_4h is None or len(df_daily) < 60 or len(df_4h) < 80:
        logger.error("NOK! Resistance zone data deficiency")
        return Res["ERR"], None

    try:
        d = _calc_boll(df_daily.copy(), window=20, std_mul=2)
        h4 = _calc_boll(df_4h.copy(), window=20, std_mul=2)

        last_d = d.iloc[-1]
        last_4h = h4.iloc[-1]
        current_price = last_4h["close"]

        score = 0
        metrics = []
        hit_count = 0

        # -------------------------
        # A. 日线前高附近
        # -------------------------
        daily_prev_high = d["high"].iloc[-60:-1].max()
        if _is_near_level(current_price, daily_prev_high, tolerance=0.015):
            score += 8
            hit_count += 1
            metrics.append("接近日线前高压力 (+8)")

        # -------------------------
        # B. 日线BOLL上轨附近
        # -------------------------
        if _is_near_level(current_price, last_d["bb_upper"], tolerance=0.012):
            score += 6
            hit_count += 1
            metrics.append("接近日线BOLL上轨 (+6)")

        # -------------------------
        # C. 4H前高附近
        # -------------------------
        h4_prev_high = h4["high"].iloc[-40:-1].max()
        if _is_near_level(current_price, h4_prev_high, tolerance=0.01):
            score += 6
            hit_count += 1
            metrics.append("接近4H前高压力 (+6)")

        # -------------------------
        # D. 4H平台上沿
        # 最近20~40根区间高点
        # -------------------------
        h4_platform_high = h4["high"].iloc[-30:-10].max()
        if _is_near_level(current_price, h4_platform_high, tolerance=0.01):
            score += 5
            hit_count += 1
            metrics.append("接近4H平台上沿 (+5)")

        # -------------------------
        # E. 4H BOLL上轨附近
        # -------------------------
        if _is_near_level(current_price, last_4h["bb_upper"], tolerance=0.008):
            score += 5
            hit_count += 1
            metrics.append("接近4H BOLL上轨 (+5)")

        # -------------------------
        # F. 4H fib 0.618 / 0.786
        # -------------------------
        fib_info = _calc_fib_retracement_zone(h4, lookback=60)
        if fib_info is not None:
            if _in_zone(current_price, fib_info["zone_low"], fib_info["zone_high"]):
                score += 5
                hit_count += 1
                metrics.append("进入4H fib 0.618~0.786回撤区 (+5)")

        # -------------------------
        # G. 共振加分
        # -------------------------
        if hit_count >= 4:
            score += 5
            metrics.append("多周期压力强共振 (+5)")
        elif hit_count >= 2:
            score += 3
            metrics.append("多周期压力共振 (+3)")
        else:
            metrics.append("压力共振一般 (+0)")

        action = StrategyResult.WAIT
        # if score >= 20:
        #     action = StrategyResult.SHORT

        return res, Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics) if metrics else "未到明确压力区",
            score=min(score, 35),
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_short_resistance_zone error: {e}")
        return Res["ERR"], None


# =========================================================
# 3. 1H 转弱信号
# 满分 25
# =========================================================
def eval_short_trigger_1h(df_1h):
    """
    Description:
    1H 是否出现转弱信号

    核心:
    1. 冲高回落
    2. 阴包阳
    3. 跌破前低
    4. 二次冲高不过前高 -> Lower High
    5. MACD 死叉 / 缩柱转绿
    """

    res = Res["OK"]

    if df_1h is None or len(df_1h) < 80:
        logger.error("NOK! 1H data deficiency")
        return Res["ERR"], None

    try:
        h1 = df_1h.copy()
        h1["ema20"] = h1["close"].ewm(span=20, adjust=False).mean()

        dif, dea, hist = _calc_macd(h1["close"])
        h1["dif"] = dif
        h1["dea"] = dea
        h1["hist"] = hist

        score = 0
        metrics = []

        last = h1.iloc[-1]
        prev = h1.iloc[-2]
        prev2 = h1.iloc[-3]

        # 1) 冲高回落：上影较长 + 收阴
        body = abs(last["close"] - last["open"]) if "open" in h1.columns else abs(last["close"] - prev["close"])
        upper_shadow = last["high"] - max(last["close"], last["open"]) if "open" in h1.columns else last["high"] - max(last["close"], prev["close"])

        if upper_shadow > body * 1.2 and last["close"] < last["open"]:
            score += 5
            metrics.append("1H冲高回落 (+5)")

        # 2) 阴包阳
        if "open" in h1.columns:
            if (
                prev["close"] > prev["open"] and
                last["close"] < last["open"] and
                last["open"] >= prev["close"] and
                last["close"] <= prev["open"]
            ):
                score += 6
                metrics.append("1H阴包阳 (+6)")

        # 3) 跌破前低
        recent_low = h1["low"].iloc[-6:-1].min()
        if last["close"] < recent_low:
            score += 6
            metrics.append("1H跌破前低 (+6)")

        # 4) 二次冲高不过前高 / Lower High
        recent_high_1 = h1["high"].iloc[-10:-5].max()
        recent_high_2 = h1["high"].iloc[-5:].max()
        if recent_high_2 < recent_high_1:
            score += 4
            metrics.append("1H二次冲高不过前高(LH) (+4)")

        # 5) MACD 死叉 / 缩柱转绿
        if last["dif"] < last["dea"] and prev["dif"] >= prev["dea"]:
            score += 2
            metrics.append("1H MACD死叉 (+2)")
        elif last["hist"] < 0 and prev["hist"] >= 0:
            score += 2
            metrics.append("1H MACD缩柱转绿 (+2)")

        # 6) 收盘重新跌回EMA20下方
        if last["close"] < last["ema20"]:
            score += 2
            metrics.append("1H跌回EMA20下方 (+2)")

        action = StrategyResult.WAIT
        # if score >= 15:
        #     action = StrategyResult.SHORT
        # elif score >= 8:
        #     action = StrategyResult.WAIT

        return res, Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics) if metrics else "1H暂未明显转弱",
            score=min(score, 25),
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_short_trigger_1h error: {e}")
        return Res["ERR"], None


# =========================================================
# 4. 止损位与风险评分
# 满分 15
# =========================================================
def eval_short_risk(df_1h, df_4h):
    """
    Description:
    止损位是否明确 + 风险是否合理

    思路:
    1. 结构止损 = 最近1H swing high 上方
    2. 用 ATR 做 buffer
    3. 风险距离太大 -> 扣分
    """

    res = Res["OK"]

    if df_1h is None or df_4h is None or len(df_1h) < 30 or len(df_4h) < 20:
        logger.error("NOK! Risk module data deficiency")
        return Res["ERR"], None

    try:
        h1 = df_1h.copy()
        h4 = df_4h.copy()

        atr_4h = _calc_atr(h4, period=14)
        atr_last = atr_4h.iloc[-1]

        current_price = h1["close"].iloc[-1]

        swing_high = _find_recent_swing_high(h1, lookback=10)
        if pd.isna(swing_high):
            return Res["ERR"], None

        stop_loss = swing_high + 0.3 * atr_last
        risk_distance = stop_loss - current_price
        risk_pct = risk_distance / (current_price + 1e-9)

        score = 0
        metrics = []

        if stop_loss > current_price:
            score += 6
            metrics.append(f"结构止损明确 stop={round(stop_loss, 4)} (+6)")
        else:
            metrics.append("止损位不合理 (+0)")

        # 风险距离评分
        if 0 < risk_pct <= 0.01:
            score += 5
            metrics.append("止损距离优秀 <=1% (+5)")
        elif 0.01 < risk_pct <= 0.02:
            score += 4
            metrics.append("止损距离合理 <=2% (+4)")
        elif 0.02 < risk_pct <= 0.03:
            score += 2
            metrics.append("止损距离偏大 <=3% (+2)")
        else:
            metrics.append("止损距离过大 (+0)")

        # 最近价格是否已太远离压力，不适合再空
        ema20_1h = h1["close"].ewm(span=20, adjust=False).mean().iloc[-1]
        stretch = (ema20_1h - current_price) / (ema20_1h + 1e-9)

        if stretch < 0.015:
            score += 4
            metrics.append("未明显追空，位置尚可 (+4)")
        else:
            metrics.append("已有追空嫌疑 (+0)")

        action = StrategyResult.WAIT
        # if score >= 10:
        #     action = StrategyResult.SHORT

        return res, Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics),
            score=min(score, 15),
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_short_risk error: {e}")
        return Res["ERR"], None
    

def eval_execution_15m():
    pass


# background 25 + resistance 35 + trigger 25 + risk 15
# =========================================================
def testsuite_result(df_1h, df_4h, df_daily):
    """
    Description:
    反弹做空总控评分

    总分说明:
    >= 90  : 重仓确认空
    80~89  : 确认空
    60~79  : 轻仓/试空
    < 60   : 等待
    """

    res = Res["OK"]

    try:
        total_score = 0
        metrics = []

        # 1. 日线空头背景
        res_bg, background = eval_short_background(df_daily, df_4h)
        if res_bg != Res["OK"] or background is None:
            logger.error("NOK! eval_short_background failed")
            return Res["ERR"], 0, None

        # 2. 日线/4H压力区
        res_zone, zone = eval_short_resistance_zone(df_daily, df_4h)
        if res_zone != Res["OK"] or zone is None:
            logger.error("NOK! eval_short_resistance_zone failed")
            return Res["ERR"], 0, None

        # 3. 1H转弱
        res_trigger, trigger = eval_short_trigger_1h(df_1h)
        if res_trigger != Res["OK"] or trigger is None:
            logger.error("NOK! eval_short_trigger_1h failed")
            return Res["ERR"], 0, None

        # 4. 风险与止损
        res_risk, risk = eval_short_risk(df_1h, df_4h)
        if res_risk != Res["OK"] or risk is None:
            logger.error("NOK! eval_short_risk failed")
            return Res["ERR"], 0, None

        parts = {
            "background": background,
            "resistance_zone": zone,
            "trigger_1h": trigger,
            "risk": risk,
        }

        for name, monitor in parts.items():
            logger.info(f"{name} -> score={monitor.score}, detail={monitor.metric}")
            total_score += monitor.score
            metrics.append(f"[{name}] {monitor.metric}")

        # -------------------------------------------------
        # 关键门槛过滤:
        # 没有背景空头 or 没到压力区 or 1H没转弱，原则上不直接给确认空
        # -------------------------------------------------
        action = StrategyResult.WAIT
        position_advice = "WAIT"

        if background.score >= 15 and zone.score >= 18 and trigger.score >= 8 and risk.score >= 8:
            if total_score >= 90:
                action = StrategyResult.SHORT
                position_advice = "重仓确认空"
            elif total_score >= 80:
                action = StrategyResult.SHORT
                position_advice = "确认空"
            elif total_score >= 60:
                action = StrategyResult.SHORT
                position_advice = "轻仓试空"
            else:
                action = StrategyResult.WAIT
                position_advice = "等待"
        else:
            action = StrategyResult.WAIT
            position_advice = "链条不完整，等待"

        summary = Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics) + f" | 仓位建议: {position_advice}",
            score=total_score,
            timestamp=_now_ts()
        )

        return res, total_score, summary.metric

    except Exception as e:
        logger.error(f"NOK! testsuite_short_result err: {e}")
        return Res["ERR"], 0, None



def eval_exit(df_1h, df_4h, entry_price):
    """
    Description:
    做空退出逻辑

    核心:
    1. 向上突破结构高点 -> 止损
    2. 价格回到EMA20上方并EMA20拐头 -> 趋势破坏退出
    3. RSI低位回升 -> 空头动能衰减退出
    4. 超跌后快速反抽 -> 主动止盈
    5. ATR trailing stop -> 兜底退出
    """

    logger.info("----Start Calculate short exit signal----")

    res = Res["ERR"]

    if df_1h is None or df_4h is None or entry_price is None:
        return res, None

    if len(df_1h) < 30 or len(df_4h) < 20:
        return res, None

    try:
        h1 = df_1h.copy()
        h4 = df_4h.copy()

        current_price = h1["close"].iloc[-1]

        # === EMA20 ===
        ema20 = h1["close"].ewm(span=20, adjust=False).mean()

        # === ATR ===
        atr = _calc_atr(h4, period=14).iloc[-1]

        # === RSI ===
        rsi = _calc_rsi(h1["close"], period=14)

        # === MACD ===
        dif, dea, hist = _calc_macd(h1["close"])

        # === structure stop ===
        recent_swing_high = _find_recent_swing_high(h1, lookback=10)
        hard_stop = recent_swing_high + 0.3 * atr if not pd.isna(recent_swing_high) else entry_price + 2 * atr

        action = StrategyResult.WAIT
        metric = "继续持有空单"

        # =================================================
        # 1. 硬止损: 向上突破结构高点
        # =================================================
        if current_price >= hard_stop:
            action = StrategyResult.EXIT
            metric = f"止损:突破结构高点 stop={round(hard_stop, 4)}"

        # =================================================
        # 2. 空头趋势被破坏: 站回EMA20且EMA20开始上拐
        # =================================================
        elif current_price > ema20.iloc[-1] and ema20.iloc[-1] > ema20.iloc[-2]:
            action = StrategyResult.EXIT
            metric = "退出:价格站回EMA20且EMA20上拐"

        # =================================================
        # 3. RSI低位回升 -> 下跌动能衰减
        # =================================================
        elif rsi.iloc[-2] < 30 and rsi.iloc[-1] > rsi.iloc[-2]:
            action = StrategyResult.EXIT
            metric = "止盈:RSI超卖后回升，空头动能衰减"

        # =================================================
        # 4. 超跌后快速反抽 / MACD拐强
        # =================================================
        elif hist.iloc[-1] > hist.iloc[-2] and hist.iloc[-2] < 0 and current_price > h1["close"].iloc[-2]:
            action = StrategyResult.EXIT
            metric = "止盈:MACD空头动能衰减，出现反抽"

        # =================================================
        # 5. ATR trailing stop
        # 对空单来说:
        #   recent_low + 2*atr
        # =================================================
        else:
            recent_low = h1["low"].rolling(10).min().iloc[-1]
            trailing_stop = recent_low + 2.0 * atr

            if current_price > trailing_stop:
                action = StrategyResult.EXIT
                metric = "止盈:ATR trailing stop 被触发"

        res = Res["OK"]

        return res, Monitor(
            StrategyResult=action,
            metric=metric,
            score=0,
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_exit_short error: {e}")
        return res, None

def eval_exit_legacy(df_4h, entry_price):
    logger.info(f"----Start Calculate selling point signal----")

    res = Res["ERR"]
     

    if df_4h is None or df_4h.empty or entry_price is None or len(df_4h) < 20:
        return res, None

    try:
        close = df_4h["close"]
        high = df_4h["high"]
        low = df_4h["low"]

        current_price = close.iloc[-1]

        # === EMA ===
        ema15 = close.ewm(span=15).mean()

        # === ATR ===
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)

        atr = tr.rolling(14).mean().iloc[-1]
        acceleration = close.pct_change().rolling(3).sum().iloc[-1]
        # === RSI(新增)===
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / (loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))

        action = StrategyResult.WAIT
        metric = "持仓中"

        # =========================
        # 1️⃣ 硬止损(保命)
        # =========================
        if current_price <= entry_price - 2.0 * atr:
            action = StrategyResult.EXIT
            metric = "止损:ATR硬止损"
        elif acceleration > 0.08: 
                action = StrategyResult.EXIT
                metric = "止盈:短期过热"
        # =========================
        # 2️⃣ 提前止盈(🔥核心优化)
        # =========================
        elif (
            rsi.iloc[-1] < rsi.iloc[-2] and   # RSI下降
            rsi.iloc[-2] > 70 and             # 曾经过热
            current_price > ema15.iloc[-1]    # 仍在高位
        ):
            action = StrategyResult.EXIT
            metric = "止盈:动能衰减(RSI)"

        # =========================
        # 3️⃣ 趋势破坏(确认)
        # =========================
        elif (
            current_price < ema15.iloc[-1] and
            ema15.iloc[-1] < ema15.iloc[-2]
        ):
            action = StrategyResult.EXIT
            metric = "止盈:趋势破坏"

        # =========================
        # 4️⃣ ATR trailing(兜底)
        # =========================
        else:
            recent_high = high.rolling(10).max().iloc[-1]
            trailing_stop = recent_high - 2.0 * atr

            if current_price < trailing_stop:
                action = StrategyResult.EXIT
                metric = "止盈:ATR回撤"

        res = Res["OK"]

        exit_signal = Monitor(
            StrategyResult=action,
            metric=metric,
            score=0,
            timestamp=curr_time
        )

        return res, exit_signal

    except Exception as e:
        logger.error(f"eval_exit error: {e}")
        return res, None
