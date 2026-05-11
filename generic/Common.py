import os
import traceback

import numpy as np
import pandas as pd
import yaml
from loguru import logger


def yml_reader(yml_file=None):
    """
    Description: Read parameters defined in yml file configuration
    :param jfile, the yml file path
    :return dict
    """
    d_yml = dict()
    if not os.path.exists(yml_file):
        logger.error("Error! Invalid yml file assigned !")
        return d_yml

    with open(yml_file, "rb") as f_yml:
        cfg = f_yml.read()
        d_yml = yaml.load(cfg, Loader=yaml.SafeLoader)

    return d_yml


def get_directional_basic_value(config, key, direction, default=None):
    basic = config.get("basic", {}) if isinstance(config, dict) else {}
    value = basic.get(key, default)
    if isinstance(value, dict):
        return value.get(direction, value.get("default", default))
    return value


def get_traceback(comment=""):
    """
    Description: Get traceback info to error log.
    """
    tb_logs = []
    if comment.strip() != "":
        tb_logs.append("++ " + comment)
        tb_logs.append(" ")

    err_stack = traceback.extract_stack()
    err_stack = err_stack[5:]
    for err_frame in err_stack:
        tb_logs.append(str(err_frame))

    err = traceback.format_exc()
    err_lines = err.splitlines()
    tb_logs.append(" ")
    tb_logs += err_lines

    logger.debug("-" * 50)
    for err_ln in tb_logs:
        if "FrameSummary" in err_ln:
            continue
        logger.debug("|> " + err_ln)
    logger.debug("-" * 50)


def calc_boll(df, window=20, std_mul=2):
    df = df.copy()
    df["bb_mid"] = df["close"].rolling(window=window).mean()
    df["bb_std"] = df["close"].rolling(window=window).std()
    df["bb_upper"] = df["bb_mid"] + std_mul * df["bb_std"]
    df["bb_lower"] = df["bb_mid"] - std_mul * df["bb_std"]
    return df


def calc_macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = dif - dea
    return dif, dea, hist


def calc_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))


def calc_atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def dynamic_tolerance(df, base_tolerance=0.005, atr_period=14):
    atr = calc_atr(df, atr_period).iloc[-1]
    price = df["close"].iloc[-1]
    if pd.isna(atr) or price <= 0:
        return base_tolerance
    vol_ratio = atr / price
    return max(base_tolerance, min(vol_ratio * 0.5, 0.02))


def is_near_level(price, level, tolerance=None, df=None):
    if pd.isna(price) or pd.isna(level) or level == 0:
        return False
    if tolerance is None:
        if df is not None:
            tolerance = dynamic_tolerance(df)
        else:
            tolerance = 0.01
    return abs(price - level) / abs(level) <= tolerance


def in_zone(price, low, high):
    if pd.isna(price) or pd.isna(low) or pd.isna(high):
        return False
    return low <= price <= high


def find_recent_swing_high(df, lookback=10):
    if df is None or len(df) < lookback:
        return np.nan
    return df["high"].iloc[-lookback:].max()


def find_recent_swing_low(df, lookback=10):
    if df is None or len(df) < lookback:
        return np.nan
    return df["low"].iloc[-lookback:].min()


def has_hh_hl_structure(df, lookback=20):
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


def calc_fib_retracement_zone(df_4h, lookback=60):
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
        "swing_low": swing_low,
    }


def calc_range_position(df, lookback=40):
    if df is None or len(df) < lookback:
        return np.nan
    recent = df.iloc[-lookback:]
    range_low = recent["low"].min()
    range_high = recent["high"].max()
    current_price = recent["close"].iloc[-1]
    if pd.isna(range_low) or pd.isna(range_high) or range_high <= range_low:
        return np.nan
    return (current_price - range_low) / (range_high - range_low + 1e-9)


def find_local_swing_points(df, left=2, right=2):
    if df is None or len(df) < left + right + 5:
        return [], []
    highs = []
    lows = []
    h = df["high"].values
    l = df["low"].values
    for i in range(left, len(df) - right):
        if h[i] == max(h[i - left:i + right + 1]):
            highs.append((i, h[i]))
        if l[i] == min(l[i - left:i + right + 1]):
            lows.append((i, l[i]))
    return highs, lows


def calc_rebound_pct(df_4h, lookback=20):
    if df_4h is None or len(df_4h) < lookback:
        return np.nan
    recent = df_4h.iloc[-lookback:]
    low = recent["low"].min()
    high = recent["high"].max()
    close = recent["close"].iloc[-1]
    if pd.isna(low) or low <= 0 or pd.isna(high):
        return np.nan
    return (close - low) / low
