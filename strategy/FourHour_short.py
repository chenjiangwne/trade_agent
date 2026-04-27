from enum import Enum
from dataclasses import dataclass
import time
from loguru import logger
import pandas as pd
import numpy as np

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


def _is_near_level(price, level, tolerance=0.01):
    if pd.isna(price) or pd.isna(level) or level == 0:
        return False
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


def _is_in_upper_range(df, current_price, lookback=40, threshold=0.8):
    """
    当前价格是否位于最近 lookback 根K线波动区间的上部区域
    threshold=0.8 表示位于上 20% 区域
    """
    if df is None or len(df) < lookback:
        return False, np.nan

    recent = df.iloc[-lookback:]
    range_low = recent["low"].min()
    range_high = recent["high"].max()

    if pd.isna(range_low) or pd.isna(range_high) or range_high <= range_low:
        return False, np.nan

    pos = (current_price - range_low) / (range_high - range_low + 1e-9)
    return pos >= threshold, pos


def _calc_rebound_pct(df_4h, lookback=20):
    """
    最近一段从低点反弹了多少
    """
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

def _calc_range_position(df, lookback=40):
    """
    当前价格位于最近区间的什么位置
    返回 0~1，越接近1越靠近区间顶部
    """
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
    """
    简单找局部高低点
    """
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
# =========================================================
# 1. 日线空头背景
# 满分 25
# =========================================================
def eval_short_background(df_daily, df_4h):
    """
    日线是否给空头背景
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
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_short_background error: {e}")
        return Res["ERR"], None



# 满分 35
# 重点：非高位不空，至少命中1个日线压力
# =========================================================
def eval_short_resistance_zone(df_daily, df_4h):
    """
    日线/4H 是否到了明确压力区
    强化版：
    1. 必须接近最近4H区间顶部（>=0.88）
    2. 必须至少命中1个日线压力
    3. 必须至少命中1个4H压力
    4. 没有明显反弹，不准按反弹空处理
    """

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

        # 1) 必须非常靠近最近4H区间顶部，防止半山腰做空
        range_pos = _calc_range_position(h4, lookback=50)
        if not pd.isna(range_pos) and range_pos >= 0.75:
            score += 10
            metrics.append(f"当前位于最近4H区间顶部 pos={round(range_pos, 3)} (+10)")
        else:
            metrics.append(f"未到4H顶部区域 pos={round(range_pos, 3) if not pd.isna(range_pos) else 'nan'} (+0)")

        rebound_pct = _calc_rebound_pct(h4, lookback=24)
        if not pd.isna(rebound_pct) and rebound_pct >= 0.03:
            score += 4
            metrics.append(f"最近4H已有反弹 rebound={round(rebound_pct * 100, 2)}% (+4)")
        else:
            metrics.append(f"反弹不足 rebound={round(rebound_pct * 100, 2) if not pd.isna(rebound_pct) else 'nan'}% (+0)")

        # 3) 日线前高附近
        daily_prev_high = d["high"].iloc[-90:-1].max()
        if _is_near_level(current_price, daily_prev_high, tolerance=0.006):
            score += 8
            daily_hit += 1
            metrics.append("接近日线前高压力 (+8)")

        # 4) 日线BOLL上轨
        if _is_near_level(current_price, last_d["bb_upper"], tolerance=0.005):
            score += 8
            daily_hit += 1
            metrics.append("接近日线BOLL上轨 (+8)")

        # 5) 4H前高附近
        h4_prev_high = h4["high"].iloc[-60:-1].max()
        if _is_near_level(current_price, h4_prev_high, tolerance=0.004):
            score += 5
            h4_hit += 1
            metrics.append("接近4H前高压力 (+5)")

        # 6) 4H平台上沿
        h4_platform_high = h4["high"].iloc[-40:-10].max()
        if _is_near_level(current_price, h4_platform_high, tolerance=0.004):
            score += 4
            h4_hit += 1
            metrics.append("接近4H平台上沿 (+4)")

        # 7) 4H BOLL上轨
        if _is_near_level(current_price, last_4h["bb_upper"], tolerance=0.004):
            score += 4
            h4_hit += 1
            metrics.append("接近4H BOLL上轨 (+4)")

        # 8) 4H fib 0.618~0.786
        fib_info = _calc_fib_retracement_zone(h4, lookback=80)
        if fib_info is not None and _in_zone(current_price, fib_info["zone_low"], fib_info["zone_high"]):
            score += 4
            h4_hit += 1
            metrics.append("进入4H fib 0.618~0.786回撤区 (+4)")

        # 9) 日线压力是硬条件
        if daily_hit >= 1:
            score += 4
            metrics.append("命中日线级别压力 (+4)")
        else:
            metrics.append("未命中日线级别压力 (+0)")

        # 10) 4H压力至少也要有一个
        if h4_hit >= 1:
            score += 2
            metrics.append("命中4H级别压力 (+2)")
        else:
            metrics.append("未命中4H级别压力 (+0)")

        # 11) 共振
        if daily_hit >= 1 and h4_hit >= 2:
            score += 3
            metrics.append("多周期压力强共振 (+3)")

        valid_zone = (
            (not pd.isna(range_pos) and range_pos >= 0.75) and
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
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_short_resistance_zone error: {e}")
        return Res["ERR"], None
def eval_short_trigger_1h(df_1h):
    """
    1H 高位转弱确认
    核心：
    1. 二次冲高不过前高
    2. 跌破双顶中间低点 / 最近摆动低点
    3. MACD转弱、阴包阳只做辅助
    4. 不满足核心结构，直接 0 分
    """

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

        recent = h1.iloc[-40:].copy()
        swing_highs, swing_lows = _find_local_swing_points(recent, left=2, right=2)

        lower_high = False
        break_neckline = False

        # =================================================
        # 1. 核心结构：二次冲高不过前高（LH）
        # =================================================
        neckline = None
        if len(swing_highs) >= 2:
            idx1, high1 = swing_highs[-2]
            idx2, high2 = swing_highs[-1]

            # 第二个高点不能高于前高，并且差距不要过大
            if high2 <= high1 * 0.998:
                lower_high = True

                # 找两个高点之间的最低点作为 neckline
                local_slice = recent.iloc[idx1:idx2 + 1]
                if len(local_slice) >= 3:
                    neckline = local_slice["low"].min()

        # =================================================
        # 2. 核心结构：跌破 neckline / 最近摆动低点
        # =================================================
        if lower_high and neckline is not None:
            if last["close"] < neckline:
                break_neckline = True

        # 兜底：如果没破 neckline，再看是否跌破最近摆动低点
        if not break_neckline:
            recent_low = h1["low"].iloc[-10:-1].min()
            if last["close"] < recent_low:
                break_neckline = True

        # =================================================
        # 3. 如果核心结构不成立，直接 0 分返回
        # =================================================
        if not (lower_high and break_neckline):
            metric_str = "未形成高位双冲失败+破位结构 -> 不空"
            return res, Monitor(
                StrategyResult=StrategyResult.WAIT,
                metric=metric_str,
                score=0,
                timestamp=_now_ts()
            )

        # =================================================
        # 4. 核心结构成立后，才开始打分
        # =================================================
        score += 10
        metrics.append("1H二次冲高不过前高(LH) (+10)")

        score += 10
        metrics.append("1H跌破双顶中间低点/最近摆动低点 (+10)")

        # 冲高回落（辅助）
        if "open" in h1.columns:
            body = abs(last["close"] - last["open"])
            upper_shadow = last["high"] - max(last["close"], last["open"])
            if upper_shadow > body * 1.2 and last["close"] < last["open"]:
                score += 2
                metrics.append("1H冲高回落 (+2)")

        # 阴包阳（辅助）
        if "open" in h1.columns:
            if (
                prev["close"] > prev["open"] and
                last["close"] < last["open"] and
                last["open"] >= prev["close"] and
                last["close"] <= prev["open"]
            ):
                score += 2
                metrics.append("1H阴包阳 (+2)")

        # MACD 死叉 / 转绿（辅助）
        if last["dif"] < last["dea"] and prev["dif"] >= prev["dea"]:
            score += 2
            metrics.append("1H MACD死叉 (+2)")
        elif last["hist"] < 0 and prev["hist"] >= 0:
            score += 2
            metrics.append("1H MACD转绿 (+2)")

        # 跌回 EMA20（辅助）
        if last["close"] < last["ema20"]:
            score += 1
            metrics.append("1H跌回EMA20下方 (+1)")

        metric_str = " | ".join(metrics)

        return res, Monitor(
            StrategyResult=StrategyResult.SHORT,
            metric=metric_str,
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
    止损位是否明确 + 风险是否合理
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
            timestamp=_now_ts()
        )

    except Exception as e:
        logger.error(f"eval_short_risk error: {e}")
        return Res["ERR"], None


# =========================================================
def testsuite_result(df_1h, df_4h, df_daily):
    """
    Description:
    反弹做空总控评分（简化版）

    逻辑：
    1. background 和 resistance_zone 先做前置过滤
    2. 满足后再执行 trigger / risk
    3. 不做仓位建议，不做复杂 action 分级
    """

    res = Res["OK"]
    total_score = 0
    metrics = []

    try:
       
        res_bg, background = eval_short_background(df_daily, df_4h)
        res_zone, zone = eval_short_resistance_zone(df_daily, df_4h)

        if res_bg != Res["OK"] or background is None:
            logger.error("NOK! eval_short_background failed")
            return Res["ERR"], total_score, None

        if res_zone != Res["OK"] or zone is None:
            logger.error("NOK! eval_short_resistance_zone failed")
            return Res["ERR"], total_score, None

        if background.score >= 14 and zone.score >= 20:

            # 背景基准分
            # 只要满足前置条件就给基础分
            # 背景越强，基准分越高，但不直接把 background.score 原样叠加
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
                "eval_short_trigger_1h": eval_short_trigger_1h(df_1h),
                "eval_short_risk": eval_short_risk(df_1h, df_4h),
            }
            for name, (result, monitor) in test_cases.items():
                logger.info(f"test_case:{name} -> execute Result is>> {result}, detail: {monitor}")

                if result != Res["OK"] or monitor is None:
                    logger.error(f"NOK! {name} execute failed")
                    return Res["ERR"], total_score, None

                total_score += monitor.score
                metrics.append(f"[{name}] {monitor.metric}")

            metric_str = " | ".join(metrics)

            return res, total_score,metric_str

        else:

            logger.warning("NOK! preconditions are not met!")

            return res, total_score, None

    except Exception as e:
        logger.error(f"NOK! testsuite_short_result err:{e}")
        return Res["ERR"], total_score, None

# =========================================================
def eval_exit(df_1h, df_4h, entry_price):
    """
    Description:
    做空退出逻辑（增强收益版）

    核心:
    1. 初始止损 = 最近结构高点 + ATR buffer
    2. 浮盈达到 1R -> 止损移动到保本
    3. 浮盈达到 1.5R -> 启动 trailing stop
    4. 价格重新站回 EMA20 且 EMA20 上拐 -> 趋势破坏退出
    5. RSI 超卖后回升 -> 只在已有较好浮盈时止盈
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
        if pd.isna(atr) or atr <= 0:
            return Res["ERR"], None

        # === RSI ===
        rsi = _calc_rsi(h1["close"], period=14)

        # === 结构高点止损 ===
        recent_swing_high = _find_recent_swing_high(h1, lookback=10)
        if pd.isna(recent_swing_high):
            return Res["ERR"], None

        initial_stop = recent_swing_high + 0.3 * atr
        initial_risk = initial_stop - entry_price

        if initial_risk <= 0:
            return Res["ERR"], None

        # === 当前浮盈 ===
        profit = entry_price - current_price
        rr = profit / (initial_risk + 1e-9)

        action = StrategyResult.WAIT
        metric = "继续持有空单"

        # =================================================
        # 1. 初始硬止损
        # =================================================
        if current_price >= initial_stop:
            action = StrategyResult.EXIT
            metric = f"止损:突破初始结构止损 stop={round(initial_stop, 4)}"

        # =================================================
        # 2. 浮盈未到 1R 前：尽量给单子呼吸，不轻易止盈
        # 只处理明显趋势破坏
        # =================================================
        elif rr < 1.0:
            if current_price > ema20.iloc[-1] and ema20.iloc[-1] > ema20.iloc[-2]:
                action = StrategyResult.EXIT
                metric = "退出:浮盈未成型，价格站回EMA20且EMA20上拐"

        # =================================================
        # 3. 浮盈达到 1R：保本保护
        # =================================================
        elif 1.0 <= rr < 1.5:
            breakeven_stop = entry_price

            if current_price >= breakeven_stop:
                action = StrategyResult.EXIT
                metric = "退出:达到1R后回撤到保本位"
            elif current_price > ema20.iloc[-1] and ema20.iloc[-1] > ema20.iloc[-2]:
                action = StrategyResult.EXIT
                metric = "止盈:达到1R后，价格站回EMA20且EMA20上拐"

        # =================================================
        # 4. 浮盈达到 1.5R：启动 trailing stop
        # trailing 不要太近，避免优秀单子被小反弹洗掉
        # =================================================
        else:
            recent_low = h1["low"].rolling(12).min().iloc[-1]
            trailing_stop = recent_low + 1.8 * atr

            if current_price >= trailing_stop:
                action = StrategyResult.EXIT
                metric = f"止盈:1.5R后触发 trailing stop={round(trailing_stop, 4)}"

            # 只有在已经有较好浮盈时，才用 RSI 回升做保护性退出
            elif rsi.iloc[-2] < 28 and rsi.iloc[-1] > rsi.iloc[-2]:
                action = StrategyResult.EXIT
                metric = "止盈:已有较好浮盈，RSI超卖后回升"

            elif current_price > ema20.iloc[-1] and ema20.iloc[-1] > ema20.iloc[-2]:
                action = StrategyResult.EXIT
                metric = "止盈:已有较好浮盈，价格站回EMA20且EMA20上拐"

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
