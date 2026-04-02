from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import ccxt
import pandas as pd
from loguru import logger


OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def _proxy_config(config: dict[str, Any]) -> dict[str, Any]:
    proxy = config.get("network", {}).get("proxy")
    if not proxy:
        return {}
    return {"http": proxy, "https": proxy}


def _build_exchange(config: dict[str, Any]) -> ccxt.Exchange:
    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "timeout": 20000,
            "proxies": _proxy_config(config),
            "options": {
                "adjustForTimeDifference": True,
                "defaultType": "spot",
            },
        }
    )
    return exchange


def _symbol_to_exchange_symbol(symbol: str) -> str:
    return symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"


def _symbol_to_file_prefix(symbol: str) -> str:
    normalized = _symbol_to_exchange_symbol(symbol)
    return normalized.replace("/", "_")


def _timeframe_hours(timeframe: str) -> int:
    if timeframe.endswith("h"):
        return int(timeframe[:-1])
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.rename(columns={column: str(column).strip().lower() for column in df.columns}).copy()
    if "datetime" in renamed.columns and "timestamp" not in renamed.columns:
        renamed = renamed.rename(columns={"datetime": "timestamp"})
    if "date" in renamed.columns and "timestamp" not in renamed.columns:
        renamed = renamed.rename(columns={"date": "timestamp"})

    missing = [column for column in OHLCV_COLUMNS if column not in renamed.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    renamed = renamed[OHLCV_COLUMNS].copy()
    renamed["timestamp"] = pd.to_datetime(renamed["timestamp"]).dt.tz_localize(None)
    for column in OHLCV_COLUMNS[1:]:
        renamed[column] = pd.to_numeric(renamed[column], errors="coerce")

    renamed = renamed.dropna(subset=OHLCV_COLUMNS).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    return renamed.reset_index(drop=True)


def _load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    if path.suffix.lower() == ".xlsx":
        return _standardize_dataframe(pd.read_excel(path))
    if path.suffix.lower() == ".csv":
        return _standardize_dataframe(pd.read_csv(path))
    raise ValueError(f"Unsupported data file: {path}")


def _save_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".xlsx":
        df.to_excel(path, index=False)
        return
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
        return
    raise ValueError(f"Unsupported output file: {path}")


def _resolve_data_file(project_root: Path, config: dict[str, Any], timeframe: str) -> Path:
    data_config = config["data"]
    explicit_key = "kline_4h_file" if timeframe == config["basic"]["timeframe_4h"] else "kline_1d_file"
    explicit = data_config.get(explicit_key)
    if explicit:
        explicit_path = project_root / explicit
        if explicit_path.exists():
            return explicit_path

    prefix = _symbol_to_file_prefix(config["basic"]["symbol"])
    if timeframe == config["basic"]["timeframe_4h"]:
        filename = f"{prefix}_3year_{timeframe}.xlsx"
    else:
        filename = f"{prefix}_3year_daily.xlsx"
    return project_root / data_config["realdata_dir"] / filename


def _fetch_latest_closed_bars(exchange: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 3) -> pd.DataFrame:
    raw_bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not raw_bars:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    frame = pd.DataFrame(raw_bars, columns=OHLCV_COLUMNS)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms").dt.tz_localize(None)
    for column in OHLCV_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    # Binance returns the in-progress bar at the end; keep only closed bars.
    return frame.iloc[:-1].reset_index(drop=True)


def _merge_incremental_data(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if incoming.empty:
        return existing
    combined = pd.concat([existing, incoming], ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
    return combined.reset_index(drop=True)


def sync_latest_ohlcv(project_root: Path, config: dict[str, Any], timeframe: str) -> Path:
    path = _resolve_data_file(project_root, config, timeframe)
    existing = _load_table(path)
    exchange = _build_exchange(config)
    symbol = _symbol_to_exchange_symbol(config["basic"]["symbol"])

    logger.info("sync {} {} -> {}", symbol, timeframe, path.name)
    incoming = _fetch_latest_closed_bars(exchange, symbol, timeframe)
    merged = _merge_incremental_data(existing, incoming)
    if merged.empty:
        raise RuntimeError(f"No data available for {symbol} {timeframe}")

    _save_table(merged, path)
    logger.info("saved {} rows to {}", len(merged), path.name)
    return path


def load_market_data(project_root: Path, config: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    timeframe_4h = config["basic"]["timeframe_4h"]
    timeframe_daily = config["basic"]["timeframe_daily"]
    data_config = config["data"]

    should_sync = bool(data_config.get("sync_on_start", True))
    if should_sync:
        sync_latest_ohlcv(project_root, config, timeframe_4h)
        time.sleep(0.2)
        sync_latest_ohlcv(project_root, config, timeframe_daily)

    df_4h = _load_table(_resolve_data_file(project_root, config, timeframe_4h))
    df_daily = _load_table(_resolve_data_file(project_root, config, timeframe_daily))

    if df_4h.empty or df_daily.empty:
        raise RuntimeError("Market data files are empty after sync")

    latest_bar_time = str(pd.to_datetime(df_4h.iloc[-1]["timestamp"]).isoformat())
    return df_4h, df_daily, latest_bar_time


def seconds_until_next_4h_bar(now: pd.Timestamp | None = None, offset_seconds: int = 5) -> int:
    current = now or pd.Timestamp.utcnow()
    current = current.tz_localize(None)
    next_bar_hour = ((current.hour // 4) + 1) * 4
    next_bar = current.normalize() + pd.Timedelta(hours=next_bar_hour)
    if next_bar_hour >= 24:
        next_bar = current.normalize() + pd.Timedelta(days=1)
    wait_seconds = max(int((next_bar - current).total_seconds()) + offset_seconds, offset_seconds)
    return wait_seconds
