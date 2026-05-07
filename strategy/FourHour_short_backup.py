from enum import Enum
from dataclasses import dataclass
import time
from loguru import logger
import pandas as pd
import numpy as np
from typing import Any
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
    fallback_value: float = 0.0
def _now_ts():
    return int(time.time() * 1000)
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
    hist = dif - dea
    return dif, dea, hist
def _calc_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))
def _calc_atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()
def _dynamic_tolerance(df, base_tolerance=0.005, atr_period=14):
    """根据ATR动态调整容忍度"""
    atr = _calc_atr(df, atr_period).iloc[-1]
    price = df["close"].iloc[-1]
    if pd.isna(atr) or price <= 0:
        return base_tolerance
    vol_ratio = atr / price
    return max(base_tolerance, min(vol_ratio * 0.5, 0.02))
def _is_near_level(price, level, tolerance=None, df=None):
    if pd.isna(price) or pd.isna(level) or level == 0:
        return False
    if tolerance is None:
        if df is not None:
            tolerance = _dynamic_tolerance(df)
        else:
            tolerance = 0.01
    return abs(price - level) / abs(level) <= tolerance
def _in_zone(price, low, high):
    if pd.isna(price) or pd.isna(low) or pd.isna(high):
        return False
    return low <= price <= high
def _find_recent_swing_high(df, lookback=10):
    if df is None or len(df) < lookback:
        return np.nan
    return df["high"].iloc[-lookback:].max()
def _has_hh_hl_structure(df, lookback=20):
    if df is None or len(df) < lookback:
        return False
    recent = df.iloc[-lookback:]
    if len(recent) < 20:
        return False
    highs_0 = recent["high"].iloc[-20:-10].max()
    highs_1 = recent["high"].iloc[-10:].max()
    lows_0 = recent["low"].iloc[-20:-10].min()
    lows_1 = recent["low"].iloc[-10:].min()
    if pd.isna(highs_0) or pd.isna(highs_1) or pd.isna(lows_0) or pd.isna(lows_1):
        return False
    return highs_1 > highs_0 and lows_1 > lows_0
def _calc_fib_retracement_zone(df_4h, lookback=60):
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
    return {
        "fib_618": fib_618,
        "fib_786": fib_786,
        "zone_low": min(fib_618, fib_786),
        "zone_high": max(fib_618, fib_786),
        "swing_high": swing_high,
        "swing_low": swing_low
    }
def _calc_range_position(df, lookback=40):
    if df is None or len(df) < lookback:
        return np.nan
    recent = df.iloc[-lookback:]
    range_low = recent["low"].min()
    range_high = recent["high"].max()
    current_price = recent["close"].iloc[-1]
    if pd.isna(range_low) or pd.isna(range_high) or range_high <= range_low:
        return np.nan
    return (current_price - range_low) / (range_high - range_low + 1e-9)
def _find_local_swing_points(df, left=2, right=2):
    if df is None or len(df) < left + right + 5:
        return [], []
    highs = []
    lows = []
    h = df["high"].values
    l = df["low"].values
    for i in range(left, len(df) - right):
        if h[i] == max(h[i-left:i+right+1]):
            highs.append((i, h[i]))
        if l[i] == min(l[i-left:i+right+1]):
            lows.append((i, l[i]))
    return highs, lows
def _calc_rebound_pct(df_4h, lookback=20):
    if df_4h is None or len(df_4h) < lookback:
        return np.nan
    recent = df_4h.iloc[-lookback:]
    low = recent["low"].min()
    high = recent["high"].max()
    close = recent["close"].iloc[-1]
    if pd.isna(low) or low <= 0 or pd.isna(high):
        return np.nan
    total_range = (high - low) / low
    rebound_pct = (close - low) / low
    return rebound_pct
# =========================================================
# 1. 日线空头背景
# =========================================================
def eval_short_background(df_daily, df_4h):
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
        if last["close"] < last["ema200"]:
            score += 10
            metrics.append("日线收盘仍在EMA200下方 (+10)")
        else:
            metrics.append("日线已站上EMA200 (+0)")
        ema200_slope = last["ema200"] - prev["ema200"]
        if ema200_slope < 0:
            score += 5
            metrics.append("日线EMA200下行 (+5)")
        else:
            metrics.append("日线EMA200未下行 (+0)")
        if last["ema50"] < last["ema200"]:
            score += 5
            metrics.append("日线EMA50 < EMA200 (+5)")
        else:
            metrics.append("日线EMA50 >= EMA200 (+0)")
        if not _has_hh_hl_structure(d, lookback=20):
            score += 5
            metrics.append("日线未形成HH+HL牛市反转结构 (+5)")
        else:
            metrics.append("日线已出现HH+HL雏形 (+0)")
        action = StrategyResult.WAIT
        if score >= 18:
            action = StrategyResult.SHORT
        return res, Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics),
            score=min(score, 25),
            timestamp=_now_ts(),
            fallback_value=0.0)
    except Exception as e:
        logger.error(f"eval_short_background error: {e}")
        return Res["ERR"], None
# =========================================================
# 2. 阻力区共振（动态容忍度）35分满分，18分空单条件，20分高位压力共振区
# =========================================================
def eval_short_resistance_zone(df_daily, df_4h):
    res = Res["OK"]
    if df_daily is None or df_4h is None or len(df_daily) < 80 or len(df_4h) < 120:
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
        daily_hit = 0
        h4_hit = 0
        atr_ratio = _calc_atr(h4, 14).iloc[-1] / current_price if current_price > 0 else 0.01
        top_threshold = 0.75 if atr_ratio < 0.02 else 0.80
        range_pos = _calc_range_position(h4, lookback=50)
        if not pd.isna(range_pos) and range_pos >= top_threshold:
            score += 10
            metrics.append(f"当前位于最近4H区间顶部 pos={round(range_pos, 3)} (动态阈>{top_threshold}) (+10)")
        else:
            metrics.append(f"未到4H顶部区域 pos={round(range_pos, 3) if not pd.isna(range_pos) else 'nan'} (+0)")
        rebound_pct = _calc_rebound_pct(h4, lookback=24)
        if not pd.isna(rebound_pct) and rebound_pct >= 0.03:
            score += 4
            metrics.append(f"最近4H已有反弹 rebound={round(rebound_pct * 100, 2)}% (+4)")
        else:
            metrics.append(f"反弹不足 rebound={round(rebound_pct * 100, 2) if not pd.isna(rebound_pct) else 'nan'}% (+0)")
        daily_prev_high = d["high"].iloc[-90:-1].max()
        if _is_near_level(current_price, daily_prev_high, df=h4):
            score += 8
            daily_hit += 1
            metrics.append("接近日线前高压力 (+8)")
        if _is_near_level(current_price, last_d["bb_upper"], df=h4):
            score += 8
            daily_hit += 1
            metrics.append("接近日线BOLL上轨 (+8)")
        h4_prev_high = h4["high"].iloc[-60:-1].max()
        if _is_near_level(current_price, h4_prev_high, df=h4):
            score += 5
            h4_hit += 1
            metrics.append("接近4H前高压力 (+5)")
        h4_platform_high = h4["high"].iloc[-40:-10].max()
        if _is_near_level(current_price, h4_platform_high, df=h4):
            score += 4
            h4_hit += 1
            metrics.append("接近4H平台上沿 (+4)")
        if _is_near_level(current_price, last_4h["bb_upper"], df=h4):
            score += 4
            h4_hit += 1
            metrics.append("接近4H BOLL上轨 (+4)")
        fib_info = _calc_fib_retracement_zone(h4, lookback=80)
        if fib_info is not None and _in_zone(current_price, fib_info["zone_low"], fib_info["zone_high"]):
            score += 4
            h4_hit += 1
            metrics.append("进入4H fib 0.618~0.786回撤区 (+4)")
        if daily_hit >= 1:
            score += 4
            metrics.append("命中日线级别压力 (+4)")
        else:
            metrics.append("未命中日线级别压力 (+0)")
        if h4_hit >= 1:
            score += 2
            metrics.append("命中4H级别压力 (+2)")
        else:
            metrics.append("未命中4H级别压力 (+0)")
        if daily_hit >= 1 and h4_hit >= 2:
            score += 3
            metrics.append("多周期压力强共振 (+3)")
        valid_zone = (
            (not pd.isna(range_pos) and range_pos >= top_threshold) and
            (not pd.isna(rebound_pct) and rebound_pct >= 0.03) and
            ((daily_hit >= 1) or (h4_hit >= 2))
        )
        action = StrategyResult.WAIT
        if valid_zone and score >= 18:
            action = StrategyResult.SHORT
        else:
            metrics.append("不是高位压力共振区 -> 禁止空")
        return res, Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics),
            score=min(score, 35),
            timestamp=_now_ts(),
            fallback_value=0.0)
    except Exception as e:
        logger.error(f"eval_short_resistance_zone error: {e}")
        return Res["ERR"], None
# =========================================================
# 3. 1H触发（成交量过滤 + 4H确认）30分满分，18分空单条件，4H确认加分但不强制
# =========================================================
def eval_short_trigger_1h(df_1h, df_4h=None):
    res = Res["OK"]
    if df_1h is None or len(df_1h) < 120:
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
        # =========================
        # 1️⃣ LH结构（前提）
        # =========================
        recent = h1.iloc[-40:]
        swing_highs, _ = _find_local_swing_points(recent, left=2, right=2)
        lower_high = False
        strong_lh = False
        if len(swing_highs) >= 2:
            high1 = swing_highs[-2][1]
            high2 = swing_highs[-1][1]
            if high2 <= high1 * 0.998:
                lower_high = True
                if high2 <= high1 * 0.995:
                    strong_lh = True
        if not lower_high:
            return res, Monitor(
                StrategyResult=StrategyResult.WAIT,
                metric="无LH结构",
                score=0,
                timestamp=_now_ts(),
                fallback_value=0.0
            )
        if strong_lh:
            score += 6
            metrics.append("强LH (+6)")
        else:
            score += 3
            metrics.append("弱LH (+3)")
        # =========================
        # 2️⃣ 连续转弱确认（🔥关键）
        # =========================
        if prev["close"] < prev["ema20"] and last["close"] < last["ema20"]:
            score += 4
            metrics.append("连续跌破EMA20 (+4)")
        if last["hist"] < 0:
            score += 2
            metrics.append("MACD转弱 (+2)")
        # =========================
        # 3️⃣ rejection（顶部失败）
        # =========================
        if "open" in h1.columns:
            body = abs(last["close"] - last["open"])
            upper_shadow = last["high"] - max(last["close"], last["open"])
            if upper_shadow > body * 1.5 and last["close"] < last["open"]:
                score += 3
                metrics.append("冲高回落 (+3)")
        # =========================
        # 4️⃣ 假突破（强信号）
        # =========================
        if len(swing_highs) >= 2:
            if last["high"] > high1 and last["close"] < high1:
                score += 4
                metrics.append("假突破失败 (+4)")
        # =========================
        # 5️⃣ 破位（加分，不强制）
        # =========================
        recent_low = h1["low"].iloc[-10:-1].min()
        if last["close"] < recent_low:
            score += 5
            metrics.append("跌破近期低点 (+5)")
        # =========================
        # 6️⃣ 成交量
        # =========================
        vol_col = None
        for col in h1.columns:
            if "volume" in col.lower() or "vol" in col.lower():
                vol_col = col
                break
        if vol_col:
            avg_vol = h1[vol_col].iloc[-25:-1].mean()
            if last[vol_col] > avg_vol * 1.2:
                score += 2
                metrics.append("放量 (+2)")
        # =========================
        # 7️⃣ 反包否定（🔥关键）
        # =========================
        if last["close"] > prev["high"]:
            score -= 5
            metrics.append("被阳线反包 (-5)")
        # =========================
        # 8️⃣ 位置过滤（🔥关键）
        # =========================
        if df_4h is not None:
            pos = _calc_range_position(df_4h, lookback=40)
            if not pd.isna(pos):
                if pos < 0.6:
                    score -= 3
                    metrics.append("位置不高 (-3)")
                elif pos > 0.8:
                    score += 2
                    metrics.append("高位区域 (+2)")
        # =========================
        # 9️⃣ 4H趋势（轻权重）
        # =========================
        if df_4h is not None and len(df_4h) >= 30:
            h4_ema20 = df_4h["close"].ewm(span=20).mean().iloc[-1]
            if last["close"] < h4_ema20:
                score += 2
                metrics.append("顺4H趋势 (+2)")
            else:
                score -= 2
                metrics.append("逆4H (-2)")
        return res, Monitor(
            StrategyResult=StrategyResult.SHORT,
            metric=" | ".join(metrics),
            score=min(score, 30),
            timestamp=_now_ts(),
            fallback_value=0.0)
    except Exception as e:
        logger.error(f"eval_short_trigger_1h error: {e}")
        return Res["ERR"], None
# 4. 风险评分
# =========================================================
def eval_short_risk(df_1h, df_4h):
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
        if pd.isna(swing_high) or pd.isna(atr_last):
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
        ema20_1h = h1["close"].ewm(span=20, adjust=False).mean().iloc[-1]
        stretch = (ema20_1h - current_price) / (ema20_1h + 1e-9)
        if stretch < 0.012:
            score += 4
            metrics.append("未明显追空，位置尚可 (+4)")
        else:
            metrics.append("已有追空嫌疑 (+0)")
        action = StrategyResult.WAIT
        if score >= 10:
            action = StrategyResult.SHORT
        return res, Monitor(
            StrategyResult=action,
            metric=" | ".join(metrics),
            score=min(score, 15),
            timestamp=_now_ts(),
            fallback_value=float(round(stop_loss, 4)))
    except Exception as e:
        logger.error(f"eval_short_risk error: {e}")
        return Res["ERR"], None
def eval_exit(df_1h, df_4h, current_price, initial_stop, current_rr, peak_rr, return_pct):
    """
    重构后的做空退出逻辑
    参数完全解耦，依赖于外部传入的精准收益率和RR
    """
    try:
        ema20 = df_1h["close"].ewm(span=20, adjust=False).mean()
        atr = _calc_atr(df_4h, period=14).iloc[-1]
        rsi = _calc_rsi(df_1h["close"], period=14)
        action = StrategyResult.WAIT
        metric = "继续持有空单"
        # 1. 绝对硬止损：亏损达到 3% 无条件斩仓 (做空亏损时 return_pct 为负数)
        hard_stop_loss_pct = -0.03
        if return_pct <= hard_stop_loss_pct:
            action = StrategyResult.EXIT
            metric = f"止损: 触发 3% 硬止损 (当前收益率: {return_pct*100:.2f}%)"
            return Res["OK"], Monitor(StrategyResult=action, metric=metric, score=0, timestamp=_now_ts(),
                fallback_value=0.0)
        # 2. 初始结构止损：价格突破了建仓时设定的最高防守线
        if current_price >= initial_stop:
            action = StrategyResult.EXIT
            metric = f"止损: 突破初始结构止损位 ({initial_stop:.2f})"
            return Res["OK"], Monitor(StrategyResult=action, metric=metric, score=0, timestamp=_now_ts(),
                fallback_value=0.0)
        # 3. 移动止盈 (Trailing Stop) 核心逻辑优化
        # 只有在产生足够的浮盈 (比如达到过 1.5R 以上) 才启动，否则很容易被震荡扫掉
        if peak_rr >= 1.5:
            # 动态收紧 ATR 乘数：利润越高，跟得越紧
            if peak_rr >= 3.0:
                atr_mult = 1.0
            elif peak_rr >= 2.0:
                atr_mult = 1.5
            else:
                atr_mult = 2.0
            # 修复版做空追踪止损：
            # 找过去 12 根 K 线的最低点，加上 ATR。因为是做空，止损线是跟着价格往下移的。
            recent_lowest_low = df_1h["low"].rolling(12).min().iloc[-1]
            trailing_stop = recent_lowest_low + (atr_mult * atr)
            # 如果反弹突破了移动止盈线，且当前依然有一定利润（例如 current_rr > 0.5 确保不是在亏本砍仓）
            if current_price >= trailing_stop and current_rr > 0.5:
                action = StrategyResult.EXIT
                metric = f"止盈: 触发移动追踪止损 (Peak RR: {peak_rr:.2f}, Stop: {trailing_stop:.2f})"
                return Res["OK"], Monitor(StrategyResult=action, metric=metric, score=0, timestamp=_now_ts(),
                fallback_value=0.0)
        # 4. 趋势破坏判定 (EMA)
        # 如果浮盈不到 1.5R 甚至还在浮亏，靠 EMA 来兜底
        if peak_rr < 1.5:
            # 价格站回 EMA20 且 EMA20 开始向上拐头
            if current_price > ema20.iloc[-1] and ema20.iloc[-1] > ema20.iloc[-2]:
                action = StrategyResult.EXIT
                metric = f"退出: 1H 趋势破坏 (价格站上上拐的 EMA20)"
                return Res["OK"], Monitor(StrategyResult=action, metric=metric, score=0, timestamp=_now_ts(),
                fallback_value=0.0)
        return Res["OK"], Monitor(StrategyResult=action, metric=metric, score=0, timestamp=_now_ts(),
                fallback_value=0.0)
    except Exception as e:
        logger.error(f"eval_exit_short error: {e}")
        return Res["ERR"], None
        
def testsuite_result(df_1h,df_4h,df_daily):
    res = Res["OK"]
    total_score = 0
    metrics = []
    risk_stop_price = []
    parameters: dict[str, Any] = {}
    try:
        res_bg, background = eval_short_background(df_daily, df_4h)
        res_zone, zone = eval_short_resistance_zone(df_daily, df_4h)
        if res_bg != Res["OK"] or background is None:
            logger.error("NOK! eval_short_background failed")
            return Res["ERR"], total_score, parameters
        if res_zone != Res["OK"] or zone is None:
            logger.error("NOK! eval_short_resistance_zone failed")
            return Res["ERR"], total_score, parameters
        if background.score >= 14 and zone.score >= 20:
            if background.score >= 22:
                bg_base_score = 4
            elif background.score >= 18:
                bg_base_score = 3
            elif background.score >= 16:
                bg_base_score = 2
            else:
                bg_base_score = 1

            test_cases = {
                "eval_short_background_base": (
                    Res["OK"],
                    Monitor(
                        StrategyResult=StrategyResult.SHORT,
                        metric=f"背景前置通过，给予背景基准分 (+{bg_base_score})",
                        score=bg_base_score,
                        timestamp=_now_ts()
                    )
                ),
                "eval_short_resistance_zone": (res_zone, zone),
                "eval_short_trigger_1h": eval_short_trigger_1h(df_1h, df_4h),
                "eval_short_risk": eval_short_risk(df_1h, df_4h),
            }
            for name, (result, monitor) in test_cases.items():
                logger.info(f"test_case:{name} -> execute Result is>> {result}, detail: {monitor}")
                if result != Res["OK"] or monitor is None:
                    logger.error(f"NOK! {name} execute failed")
                    return Res["ERR"], total_score, parameters
                total_score += monitor.score
                metrics.append(f"[{name}] {monitor.metric}")
                risk_stop_price.append(f"[{name}] {monitor.fallback_value}")
            metric_str = " | ".join(metrics)
            parameters["metric_str"] = metric_str
            parameters["risk_stop_price"] = risk_stop_price
            return res, total_score, parameters
        else:
            logger.warning("NOK! preconditions are not met!")
            return res, total_score, parameters
    except Exception as e:
        logger.error(f"NOK! testsuite_short_result err:{e}")
        return Res["ERR"], total_score, parameters
def _find_recent_swing_high(df, lookback=10):
    """
    寻找近期 N 根 K 线的最高点 (摆动高点)
    """
    if df is None or len(df) < lookback:
        return np.nan
        
    # 直接在 'high' 列中截取最后 lookback 行，并求最大值
    return df["high"].iloc[-lookback:].max()
def _calc_atr(df, period=14):
    """
    计算 ATR (平均真实波动幅度)
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    
    # TR (True Range) 是以下三个值中的最大值：
    # 1. 当天最高价 - 当天最低价
    # 2. 绝对值(当天最高价 - 昨天收盘价)
    # 3. 绝对值(当天最低价 - 昨天收盘价)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    
    # 对 TR 进行 period 周期的移动平均，得出 ATR
    return tr.rolling(period).mean()
def calc_short_performance(entry_price, current_price, stop_loss_price=None):
    """
    计算做空仓位的表现 (收益率与盈亏比)
    
    参数:
        entry_price: 初始建仓均价
        current_price: 当前价格或平仓价格
        stop_loss_price: 初始结构止损价 (可选，用于计算 RR)
        
    返回:
        dict: 包含 'return_pct' (收益率) 和 'rr' (盈亏比，如果未提供止损价则为 None)
    """
    if entry_price is None or entry_price <= 0:
        return {"return_pct": 0.0, "rr": None}
        
    # 1. 计算基础百分比收益率 (做空逻辑)
    return_pct = (entry_price - current_price) / entry_price
    
    # 2. 计算盈亏比 (RR)
    rr = None
    if stop_loss_price is not None:
        initial_risk = stop_loss_price - entry_price
        
        # 防止止损价等于建仓价导致除以 0 的异常
        if initial_risk > 0: 
            current_profit = entry_price - current_price
            rr = current_profit / initial_risk
        elif initial_risk < 0:
            # 如果做空的止损价居然低于建仓价，说明逻辑错误，记录异常
            # logger.warning("止损价低于做空建仓价，逻辑异常！")
            rr = 0.0
            
    return {
        "return_pct": return_pct,
        "rr": rr
    }
# # --- 使用示例 ---
# # 假设你在 70000 建仓做空，止损放在 72100 (风险约 3%)，现在跌到了 65000
# result = calc_short_performance(entry_price=70000, current_price=65000, stop_loss_price=72100)
# print(f"当前收益率: {result['return_pct'] * 100:.2f}%")  # 输出: 7.14%
# print(f"当前盈亏比 (RR): {result['rr']:.2f} R")        # 输出: 2.38 R
