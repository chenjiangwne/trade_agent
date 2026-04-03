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


def fetch_exchange_clock(config: dict[str, Any]) -> dict[str, Any]:
    exchange = _build_exchange(config)
    exchange_time_ms = exchange.fetch_time()
    if exchange_time_ms is None:
        raise RuntimeError("failed to fetch exchange time")

    exchange_now = pd.to_datetime(exchange_time_ms, unit="ms").tz_localize(None)
    local_now = pd.Timestamp.utcnow().tz_localize(None)
    diff_seconds = abs((local_now - exchange_now).total_seconds())
    max_diff_seconds = int(config.get("runtime", {}).get("max_clock_diff_seconds", 60))
    if diff_seconds > max_diff_seconds:
        raise RuntimeError(
            f"local clock drift too large: local={local_now.isoformat()} exchange={exchange_now.isoformat()} diff={diff_seconds:.0f}s"
        )

    logger.info(
        "exchange clock ok: exchange_now={} local_now={} diff_seconds={:.1f}",
        exchange_now.isoformat(),
        local_now.isoformat(),
        diff_seconds,
    )
    return {
        "exchange_now": exchange_now,
        "local_now": local_now,
        "diff_seconds": diff_seconds,
    }


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


def _expected_delta(timeframe: str) -> pd.Timedelta:
    if timeframe.endswith("h"):
        return pd.Timedelta(hours=int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return pd.Timedelta(days=int(timeframe[:-1]))
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _timeframe_milliseconds(timeframe: str) -> int:
    return int(_expected_delta(timeframe).total_seconds() * 1000)


def _ensure_ohlc_sanity(df: pd.DataFrame, label: str) -> None:
    invalid = (
        (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
        | (df["high"] < df["low"])
        | (df["volume"] < 0)
    )
    if invalid.any():
        bad_rows = int(invalid.sum())
        raise RuntimeError(f"{label} contains {bad_rows} invalid OHLCV rows")


def _continuous_tail(df: pd.DataFrame, timeframe: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    expected = _expected_delta(timeframe)
    diffs = df["timestamp"].diff()
    bad_gaps = diffs[(diffs.notna()) & (diffs != expected)]
    if bad_gaps.empty:
        return df.reset_index(drop=True), {
            "gap_count": 0,
            "tail_rows": len(df),
            "last_gap_at": None,
        }

    last_break_index = int(bad_gaps.index[-1])
    tail = df.iloc[last_break_index:].reset_index(drop=True)
    return tail, {
        "gap_count": int(len(bad_gaps)),
        "tail_rows": len(tail),
        "last_gap_at": str(df.iloc[last_break_index]["timestamp"]),
    }


def _last_gap_repair_since(existing: pd.DataFrame, timeframe: str) -> int | None:
    if existing.empty:
        return None

    expected = _expected_delta(timeframe)
    diffs = existing["timestamp"].diff()
    bad_gaps = diffs[(diffs.notna()) & (diffs != expected)]
    if bad_gaps.empty:
        last_ts = pd.Timestamp(existing.iloc[-1]["timestamp"])
        return int(last_ts.timestamp() * 1000) - _timeframe_milliseconds(timeframe)

    last_gap_index = int(bad_gaps.index[-1])
    prev_index = max(last_gap_index - 1, 0)
    prev_ts = pd.Timestamp(existing.iloc[prev_index]["timestamp"])
    return int(prev_ts.timestamp() * 1000)


def _validate_market_frame(
    df: pd.DataFrame,
    timeframe: str,
    min_rows: int,
    label: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if df.empty:
        raise RuntimeError(f"{label} is empty")

    if df["timestamp"].isna().any():
        raise RuntimeError(f"{label} contains invalid timestamp values")

    if not df["timestamp"].is_monotonic_increasing:
        raise RuntimeError(f"{label} timestamps are not sorted ascending")

    if df["timestamp"].duplicated().any():
        duplicate_count = int(df["timestamp"].duplicated().sum())
        raise RuntimeError(f"{label} contains {duplicate_count} duplicated timestamps")

    _ensure_ohlc_sanity(df, label)
    continuous_df, report = _continuous_tail(df, timeframe)
    if len(continuous_df) < min_rows:
        raise RuntimeError(
            f"{label} continuous tail is too short: {len(continuous_df)} rows, require at least {min_rows}"
        )

    if report["gap_count"] > 0:
        logger.warning(
            "{} has {} discontinuities; only the latest continuous tail is used, rows={}, last_gap_at={}",
            label,
            report["gap_count"],
            report["tail_rows"],
            report["last_gap_at"],
        )
    else:
        logger.info("{} validation passed with {} continuous rows", label, report["tail_rows"])

    return continuous_df, report


def _sync_required_timeframes(project_root: Path, config: dict[str, Any], exchange_now: pd.Timestamp) -> None:
    timeframe_4h = config["basic"]["timeframe_4h"]
    timeframe_daily = config["basic"]["timeframe_daily"]
    sync_latest_ohlcv(project_root, config, timeframe_4h, exchange_now=exchange_now)
    time.sleep(0.2)
    sync_latest_ohlcv(project_root, config, timeframe_daily, exchange_now=exchange_now)


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


def _fetch_closed_bars(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    exchange_now: pd.Timestamp,
    since: int | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    raw_bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
    if not raw_bars:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    frame = pd.DataFrame(raw_bars, columns=OHLCV_COLUMNS)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms").dt.tz_localize(None)
    for column in OHLCV_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    close_time = frame["timestamp"] + _expected_delta(timeframe)
    closed_frame = frame.loc[close_time <= exchange_now].reset_index(drop=True)
    return closed_frame


def _fetch_incremental_bars(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    existing: pd.DataFrame,
    exchange_now: pd.Timestamp,
) -> pd.DataFrame:
    if existing.empty:
        logger.info("no local {} data, fetch latest window from exchange", timeframe)
        return _fetch_closed_bars(exchange, symbol, timeframe, exchange_now=exchange_now, since=None, limit=1000)

    step_ms = _timeframe_milliseconds(timeframe)
    since = _last_gap_repair_since(existing, timeframe)
    logger.info("local {} data exists, fetch incremental bars since={}", timeframe, pd.to_datetime(since, unit="ms"))
    batches: list[pd.DataFrame] = []

    for _ in range(20):
        batch = _fetch_closed_bars(exchange, symbol, timeframe, exchange_now=exchange_now, since=since, limit=1000)
        if batch.empty:
            break

        batches.append(batch)
        last_ts_ms = int(pd.Timestamp(batch.iloc[-1]["timestamp"]).timestamp() * 1000)
        next_since = last_ts_ms + step_ms
        if next_since <= since:
            break
        since = next_since

        if len(batch) < 1000:
            break

        time.sleep(exchange.rateLimit / 1000)

    if not batches:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    return pd.concat(batches, ignore_index=True)


def _merge_incremental_data(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if incoming.empty:
        return existing
    combined = pd.concat([existing, incoming], ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
    return combined.reset_index(drop=True)


def sync_latest_ohlcv(project_root: Path, config: dict[str, Any], timeframe: str, exchange_now: pd.Timestamp) -> Path:
    path = _resolve_data_file(project_root, config, timeframe)
    existing = _load_table(path)
    exchange = _build_exchange(config)
    symbol = _symbol_to_exchange_symbol(config["basic"]["symbol"])

    logger.info("sync {} {} -> {}", symbol, timeframe, path.name)
    incoming = _fetch_incremental_bars(exchange, symbol, timeframe, existing, exchange_now=exchange_now)
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
    validation_config = data_config.get("validation", {})
    min_rows_4h = int(validation_config.get("min_rows_4h", 200))
    min_rows_1d = int(validation_config.get("min_rows_1d", 60))
    max_sync_attempts = int(validation_config.get("max_sync_attempts", 3))
    should_sync = bool(data_config.get("sync_on_start", True))

    last_error: Exception | None = None
    total_attempts = max_sync_attempts if should_sync else 1

    for attempt in range(1, total_attempts + 1):
        try:
            clock = fetch_exchange_clock(config)
            exchange_now = clock["exchange_now"]
            if should_sync:
                logger.info("market data validation cycle {}/{}: sync from exchange first", attempt, total_attempts)
                _sync_required_timeframes(project_root, config, exchange_now=exchange_now)

            df_4h = _load_table(_resolve_data_file(project_root, config, timeframe_4h))
            df_daily = _load_table(_resolve_data_file(project_root, config, timeframe_daily))

            if df_4h.empty or df_daily.empty:
                raise RuntimeError("market data files are empty after sync")

            df_4h, report_4h = _validate_market_frame(df_4h, timeframe_4h, min_rows_4h, "4h market data")
            df_daily, report_1d = _validate_market_frame(df_daily, timeframe_daily, min_rows_1d, "1d market data")

            logger.info(
                "market data ready: 4h_rows={} 1d_rows={} 4h_gaps={} 1d_gaps={}",
                len(df_4h),
                len(df_daily),
                report_4h["gap_count"],
                report_1d["gap_count"],
            )

            latest_bar_time = str(pd.to_datetime(df_4h.iloc[-1]["timestamp"]).isoformat())
            return df_4h, df_daily, latest_bar_time
        except Exception as exc:
            last_error = exc
            logger.error("market data validation failed on attempt {}/{}: {}", attempt, total_attempts, exc)
            if attempt >= total_attempts:
                break
            time.sleep(1)

    raise RuntimeError(f"market data validation failed after {total_attempts} attempts: {last_error}")


def seconds_until_next_4h_bar(now: pd.Timestamp | None = None, offset_seconds: int = 5) -> int:
    current = now or pd.Timestamp.utcnow()
    current = current.tz_localize(None)
    next_bar_hour = ((current.hour // 4) + 1) * 4
    next_bar = current.normalize() + pd.Timedelta(hours=next_bar_hour)
    if next_bar_hour >= 24:
        next_bar = current.normalize() + pd.Timedelta(days=1)
    wait_seconds = max(int((next_bar - current).total_seconds()) + offset_seconds, offset_seconds)
    return wait_seconds


def seconds_until_next_4h_bar_by_exchange(config: dict[str, Any], offset_seconds: int = 5) -> int:
    clock = fetch_exchange_clock(config)
    return seconds_until_next_4h_bar(now=clock["exchange_now"], offset_seconds=offset_seconds)
