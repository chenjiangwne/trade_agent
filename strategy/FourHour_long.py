
from enum import Enum
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import time
from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
Res = {"OK": 0, 'ERR': -1,"EXCEPTION":-2}
class StrategyResult(Enum):
    WAIT = "WAIT"        
    LONG = "LONG"        
    SHORT = "SHORT"     
    EXIT = "EXIT"        
    ERROR = "ERROR"      

@dataclass
class Monitor:
    StrategyResult:StrategyResult
    metric:str
    score:int
    timestamp:str
curr_time = int(time.time() * 1000)

def is_system_ready(df_4h, df_daily):
    
    if len(df_4h) < 200 or len(df_daily) < 40:
        logger.error("NOK! Daily data deficiency")
        return False
    
    last_ts = df_4h.iloc[-1]['timestamp']
    if ((time.time() * 1000) - last_ts.timestamp() * 1000) > (4 * 3600 * 1000 * 1.5):
        logger.error("NOK! Data is stale (Outdated)")
        return True

    # 3. 这里的逻辑可以比作 ADAS 里的传感器 Self-Test
    return True

def eval_trend(df_4h, df_daily):
    """
    Description: 看大势 - 趋势过滤系统 (Long Only)
    """
    
    res = Res["OK"]
    
    if is_system_ready(df_4h, df_daily):
        logger.error("NOK! Daily data deficiency")
        return Res["ERR"], None

    score = 0
    metrics = []

    # =========================
    # 1 日线趋势判断
    # =========================

    df_daily["ema200"] = df_daily["close"].ewm(span=200, adjust=False).mean()

    last_day = df_daily.iloc[-1]
    prev_day = df_daily.iloc[-2]

    ema_slope = last_day["ema200"] - prev_day["ema200"]

    if last_day["close"] > last_day["ema200"]:

        if ema_slope > 0:
            score += 4
            metrics.append("日线EMA200上方且向上 (+4)")

        else:
            score += 2
            metrics.append("日线EMA200上方但走平 (+2)")

    else:
        metrics.append("日线EMA200下方 (趋势过滤失败)")

    # =========================
    # 2 4H趋势确认
    # =========================

    df_4h["ema50"] = df_4h["close"].ewm(span=50, adjust=False).mean()
    df_4h["ema200"] = df_4h["close"].ewm(span=200, adjust=False).mean()

    last_4h = df_4h.iloc[-1]
    prev_4h = df_4h.iloc[-2]

    ema50_slope = last_4h["ema50"] - prev_4h["ema50"]

    if last_4h["ema50"] > last_4h["ema200"]:
        score += 2
        metrics.append("4H EMA50 > EMA200 (+2)")
    else:
        metrics.append("4H EMA50 < EMA200 (+0)")

    if last_4h["close"] > last_4h["ema50"]:
        score += 1
        metrics.append("价格站上EMA50 (+1)")
    else:
        metrics.append("价格跌破EMA50 (+0)")

    if ema50_slope > 0:
        score += 1
        metrics.append("EMA50上升 (+1)")


    recent_high = df_4h["high"].iloc[-10:].max()
    prev_high = df_4h["high"].iloc[-20:-10].max()

    if recent_high > prev_high:
        score += 1
        metrics.append("结构创新高 (+1)")
    else:
        metrics.append("未创新高 (+0)")

    trend = Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=metrics,
        score=score,
        timestamp=curr_time
    )
    return res, trend


def eval_momentum(df_4h):
    """
    Description:势能评分 (Momentum Factor)
    逻辑结构:
    1. 趋势确认:EMA15 > EMA50
    2. 动能确认:价格突破或远离 EMA15
    3. 斜率确认:EMA15 上升斜率足够
    4. 波动过滤:距离 EMA15 > 0.3 ATR

    只捕捉做多动能爆发
    """
    res = Res["OK"]
     
    close = df_4h["close"]

    ema15 = close.ewm(span=15).mean()
    ema50 = close.ewm(span=50).mean()

    price = close.iloc[-1]

    score = 0
    metrics = []

    if ema15.iloc[-1] > ema50.iloc[-1]:
        score += 1
        metrics.append("EMA15 > EMA50,动能强劲")

    slope = ema15.iloc[-1] - ema15.iloc[-2]

    if slope > 0:
        score += 1
        metrics.append("EMA15 上升")

    if price > ema15.iloc[-1]:
        score += 1
        metrics.append("价格高于 EMA15")

    return res, Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=" | ".join(metrics),
        score=score,
        timestamp=curr_time
    )

def eval_position(df_4h):
    """
    Description: 位置评分 - 衡量价格偏离均值程度(钟摆偏离模型)
    """

    res = Res['OK']
     

    # ===== 布林带计算 =====
    df_4h['bb_mid'] = df_4h['close'].rolling(window=20).mean()
    df_4h['bb_std'] = df_4h['close'].rolling(window=20).std()
    df_4h['bb_lower'] = df_4h['bb_mid'] - (2 * df_4h['bb_std'])

    # ===== Z-score(偏离度)=====
    df_4h['zscore'] = (df_4h['close'] - df_4h['bb_mid']) / df_4h['bb_std']

    last = df_4h.iloc[-1]
    prev = df_4h.iloc[-2]

    score = 0
    metrics = []

    z = last['zscore']

    if z <= -2.5:
        score += 4
        metrics.append("极端超跌 Z<-2.5")

    elif z <= -2:
        score += 3
        metrics.append("严重超跌 Z<-2")

    elif z <= -1.5:
        score += 2
        metrics.append("明显低估 Z<-1.5")

    elif z <= -1:
        score += 1
        metrics.append("轻微低估 Z<-1")

    # 2 布林下轨确认
    if last['close'] <= last['bb_lower']:
        score += 1
        metrics.append("触及布林下轨")

    # 3 跌破后回归确认
    if prev['close'] < prev['bb_lower'] and last['close'] > last['bb_lower']:
        score += 2
        metrics.append("跌破后重新收回布林带")

    # 4 均值距离
    distance = (last['bb_mid'] - last['close']) / last['bb_mid']

    if distance > 0.05:
        score += 2
        metrics.append("价格远离均值 >5%")

    elif distance > 0.03:
        score += 1
        metrics.append("价格偏离均值 >3%")

    metric_str = " | ".join(metrics) if metrics else "位置中性"

    return res, Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=f"{metric_str} (+{score})",
        score=score,
        timestamp=curr_time
    )



def eval_rsi(df_4h):
    """
    Description:RSI情绪评分,定拐点。RSI 从超卖区回勾,增加博弈胜率。
    逻辑:
    1. RSI处于超卖区间 (<35)
    2. RSI开始回升
    3. 出现短期拐点
    """ 
    res = Res['OK']
     

    close = df_4h['close']

    # Wilder RSI (更接近真实指标)
    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()

    rs = avg_gain / (avg_loss + 1e-9)
    df_4h['rsi'] = 100 - (100 / (1 + rs))

    # 最近三个周期
    rsi_t = df_4h['rsi'].iloc[-1]
    rsi_t1 = df_4h['rsi'].iloc[-2]
    rsi_t2 = df_4h['rsi'].iloc[-3]
    logger.debug(df_4h['rsi'])
    # RSI拐点 + 超卖
    oversold = rsi_t < 40
    rebound = rsi_t > rsi_t1
    turning_point = rsi_t1 < rsi_t2 and rsi_t > rsi_t1

    if oversold and rebound and turning_point:
        emotion = Monitor(
            StrategyResult=StrategyResult.WAIT,
            metric="RSI超卖并出现反弹拐点 (+1)",
            score=1,
            timestamp=curr_time
        )
    else:
        emotion = Monitor(
            StrategyResult=StrategyResult.WAIT,
            metric="RSI情绪正常 (+0)",
            score=0,
            timestamp=curr_time
        )

    return res, emotion

def eval_exit(df_4h, entry_price):
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


def testsuite_result(df_4h,df_daily):
    res = Res['OK']
    total_socres=0
    metirce=[]
    try:
        res,regime=eval_regime(df_4h)
        res,position=eval_position(df_4h)
        
        if (regime.score) >=3 and (position.score) >=2:

            test_cases = {
                "eval_trend": (eval_trend(df_4h, df_daily)),
                "eval_momentum": (eval_momentum(df_4h)),
                "eval_position": (eval_position(df_4h)),
                "eval_rsi": (eval_rsi(df_4h)),
                "eval_regime": (eval_regime(df_4h)),
            } 

            for name, (result,trend) in test_cases.items():
                logger.info(f"test_case:{name} -> execute Result is>> {result}, Trend detail: {trend}")
                total_socres+=(trend.score)
                metrics +=(trend.trend)
            if all(val[0] == Res["OK"] for val in test_cases.values()):
        
                return res,total_socres,metrics
            else:
                logger.error("NOK! The case failed to occur,Please check log")  
                res = Res['OK']
                return res,total_socres,None
        else:
            logger.info(f"NOK! The preconditions are not met,the score is>>{regime.score+position.score}<<")
            return res,total_socres,None
    except Exception as e:
        logger.error(f"NOK! err:{e}")
        return Res["ERR"] ,total_socres,None
    

#v1.2
def eval_regime(df_4h):
    """
    Description: 市场状态过滤(增强版)
    """

    res = Res["OK"]
     

    high = df_4h["high"]
    low = df_4h["low"]
    close = df_4h["close"]

    score = 0
    metrics = []

    # ===== EMA =====
    ema15 = close.ewm(span=15).mean()
    ema50 = close.ewm(span=50).mean()

    # ===== ATR =====
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    atr_mean = atr.rolling(50).mean()

    atr_ratio = atr.iloc[-1] / (atr_mean.iloc[-1] + 1e-9)

    if atr_ratio > 1.2:
        score += 2
        metrics.append("ATR扩张")
    elif atr_ratio > 1.0:
        score += 1
        metrics.append("ATR正常")
    else:
        metrics.append("ATR收缩")

    # ===== ADX =====
    plus_dm = high.diff().clip(lower=0)
    minus_dm = (-low.diff()).clip(lower=0)

    tr_smooth = tr.ewm(alpha=1/14).mean()

    plus_di = 100 * (plus_dm.ewm(alpha=1/14).mean() / (tr_smooth + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/14).mean() / (tr_smooth + 1e-9))

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    adx = dx.ewm(alpha=1/14).mean()

    adx_last = adx.iloc[-1]

    # ===== 趋势强度 =====
    if adx_last > 25:
        score += 2
        metrics.append("强趋势")
    elif adx_last > 20:
        score += 1
        metrics.append("弱趋势")
    else:
        metrics.append("震荡市场")

    # ===== 🔥趋势末期过滤(核心新增)=====
    trend_count = (close > ema50).rolling(20).sum().iloc[-1]

    if trend_count > 15:
        score -= 2   # ❗直接扣分,而不是只提示
        metrics.append("趋势过长(禁止追多)")

    # ===== EMA扩散 =====
    spread = abs(ema15.iloc[-1] - ema50.iloc[-1]) / (ema50.iloc[-1] + 1e-9)

    if spread > 0.01:
        score += 1
        metrics.append("EMA扩散")
    else:
        metrics.append("EMA压缩")

    return res, Monitor(
        StrategyResult=StrategyResult.WAIT,
        metric=" | ".join(metrics),
        score=score,
        timestamp=curr_time
    )
# 
# 
# 
# 
def eval_execution_15m(df_15m):
    """
    Description: 15m执行层(入场确认)
    只用于过滤,不参与score叠加
    """
    res = Res["OK"]
     
    if df_15m is None or len(df_15m) < 50:
        return Res["ERR"], None

    close = df_15m["close"]
    high = df_15m["high"]

    score = 0
    metrics = []

    # 1️⃣ 连续上涨动量(避免接飞刀)
    # =========================
    if close.iloc[-1] > close.iloc[-2] > close.iloc[-3]:
        score += 1
        metrics.append("15m连续上涨")

    # 2️⃣ 站上EMA20(短期趋势转强)
    # =========================
    ema20 = close.ewm(span=20).mean()

    if close.iloc[-1] > ema20.iloc[-1]:
        score += 1
        metrics.append("站上EMA20")

    # 3️⃣ 突破短期高点(结构突破)
    # =========================
    recent_high = high.iloc[-5:-1].max()

    if close.iloc[-1] > recent_high:
        score += 1
        metrics.append("突破短期高点")


    # ✔ 判定逻辑(关键)
    # =========================
    if score >= 1:
        action = StrategyResult.LONG
        metric = "15m确认通过: " + " | ".join(metrics)
    else:
        action = StrategyResult.WAIT
        metric = "15m未确认"

    return res, Monitor(
        StrategyResult=action,
        metric=metric,
        score=score,
        timestamp=curr_time
    )