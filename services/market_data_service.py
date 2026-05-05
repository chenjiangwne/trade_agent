from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any

import ccxt
import pandas as pd
from loguru import logger


OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
BEIJING_TZ = "Asia/Shanghai"


def _switch_clash_node_for_failover(project_root: Path, config: dict[str, Any]) -> bool:
    failover = config.get("network", {}).get("clash_failover", {})
    if not bool(failover.get("enabled", True)):
        return False

    script_path = project_root / "tools" / "switch_clash_node.py"
    if not script_path.exists():
        logger.warning("clash failover script not found: {}", script_path)
        return False

    controller = str(failover.get("controller", "http://127.0.0.1:9097"))
    secret = str(failover.get("secret", "fdasfasfdaddf"))
    group = str(failover.get("group", "GLOBAL"))
    node = str(failover.get("node", "AUTO"))
    timeout_seconds = int(failover.get("timeout_seconds", 15))

    cmd = [
        "python",
        str(script_path),
        "--controller",
        controller,
        "--secret",
        secret,
        "--group",
        group,
        "--node",
        node,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if result.returncode != 0:
            logger.warning(
                "clash failover switch failed: code={} stdout={} stderr={}",
                result.returncode,
                stdout,
                stderr,
            )
            return False
        logger.info("clash failover switched node successfully: {}", stdout)
        return True
    except Exception as exc:
        logger.warning("clash failover switch exception: {}", exc)
        return False


def _can_reach_binance(config: dict[str, Any], timeout_ms: int = 8000) -> bool:
    try:
        exchange = ccxt.binance(
            {
                "enableRateLimit": True,
                "timeout": timeout_ms,
                "proxies": _proxy_config(config),
                "options": {
                    "adjustForTimeDifference": True,
                    "defaultType": "future",
                },
            }
        )
        server_time = exchange.fetch_time()
        return server_time is not None
    except Exception as exc:
        logger.warning("binance connectivity check failed: {}", exc)
        return False


def _try_clash_failover_and_wait_binance(project_root: Path, config: dict[str, Any]) -> bool:
    failover = config.get("network", {}).get("clash_failover", {})
    max_rounds = max(int(failover.get("max_rounds", 3)), 1)
    warmup_seconds = max(float(failover.get("warmup_seconds", 3.0)), 0.5)
    check_timeout_ms = max(int(failover.get("check_timeout_ms", 8000)), 1000)
    group = str(failover.get("group", "GLOBAL"))

    node_candidates = failover.get("nodes")
    if not isinstance(node_candidates, list) or not node_candidates:
        node_candidates = [str(failover.get("node", "AUTO"))]
    node_candidates = [str(node) for node in node_candidates if str(node).strip()]
    if not node_candidates:
        node_candidates = ["AUTO"]

    for round_idx in range(max_rounds):
        node = node_candidates[round_idx % len(node_candidates)]
        logger.warning(
            "market data failover round {}/{}: switch clash group={} node= {}",
            round_idx + 1,
            max_rounds,
            group,
            node,
        )

        local_cfg = {
            **config,
            "network": {
                **config.get("network", {}),
                "clash_failover": {
                    **failover,
                    "group": group,
                    "node": node,
                },
            },
        }
        switched = _switch_clash_node_for_failover(project_root, local_cfg)
        if not switched:
            continue

        time.sleep(warmup_seconds)
        if _can_reach_binance(config, timeout_ms=check_timeout_ms):
            logger.info("binance connectivity recovered after clash failover")
            return True

    logger.error("clash failover exhausted: binance still unreachable after {} rounds", max_rounds)
    return False


def _proxy_config(config: dict[str, Any]) -> dict[str, Any]:
    proxy = config.get("network", {}).get("proxy")
    if not proxy:
        return {}
    return {"http": proxy, "https": proxy}


def _exchange_class(name: str):
    exchange_name = str(name).lower()
    exchange_class = getattr(ccxt, exchange_name, None)
    if exchange_class is None:
        raise ValueError(f"Unsupported exchange: {name}")
    return exchange_class


def _exchange_names(config: dict[str, Any]) -> list[str]:
    primary = str(config["basic"].get("platform", "binance"))
    backups = [str(item) for item in config.get("data", {}).get("backup_platforms", []) if str(item).strip()]
    ordered = [primary, *backups]
    deduped: list[str] = []
    for name in ordered:
        lowered = name.lower()
        if lowered not in deduped:
            deduped.append(lowered)
    return deduped


def _build_exchange(config: dict[str, Any], exchange_name: str | None = None) -> ccxt.Exchange:
    exchange_class = _exchange_class(exchange_name or config["basic"].get("platform", "binance"))
    exchange = exchange_class(
        {
            "enableRateLimit": True,
            "timeout": 20000,
            "proxies": _proxy_config(config),
            "options": {
                "adjustForTimeDifference": True,
                "defaultType": "future",
            },
        }
    )
    return exchange


def fetch_exchange_clock(config: dict[str, Any]) -> dict[str, Any]:
    exchange_names = _exchange_names(config)
    last_error: Exception | None = None
    for exchange_name in exchange_names:
        try:
            exchange = _build_exchange(config, exchange_name=exchange_name)
            exchange_time_ms = exchange.fetch_time()
            if exchange_time_ms is None:
                raise RuntimeError("failed to fetch exchange time")

            exchange_now = pd.to_datetime(exchange_time_ms, unit="ms", utc=True).tz_convert(BEIJING_TZ).tz_localize(None)
            local_now = pd.Timestamp.now(tz=BEIJING_TZ).tz_localize(None)
            diff_seconds = abs((local_now - exchange_now).total_seconds())
            max_diff_seconds = int(config.get("runtime", {}).get("max_clock_diff_seconds", 60))
            if diff_seconds > max_diff_seconds:
                raise RuntimeError(
                    f"local clock drift too large: local={local_now.isoformat()} exchange={exchange_now.isoformat()} diff={diff_seconds:.0f}s"
                )

            logger.info(
                "exchange clock ok: source={} exchange_now={} local_now={} diff_seconds={:.1f}",
                exchange_name,
                exchange_now.isoformat(),
                local_now.isoformat(),
                diff_seconds,
            )
            return {
                "exchange_now": exchange_now,
                "local_now": local_now,
                "diff_seconds": diff_seconds,
                "exchange_name": exchange_name,
            }
        except Exception as exc:
            last_error = exc
            logger.warning("exchange clock health check failed for {}: {}", exchange_name, exc)

    raise RuntimeError(f"all exchange clock health checks failed: {last_error}")


def _symbol_to_exchange_symbol(symbol: str) -> str:
    if ":" in symbol and "/" in symbol:
        return symbol
    if "/" in symbol:
        base, quote = symbol.split("/", 1)
        return f"{base}/{quote}:{quote}"
    quote = symbol[-4:]
    base = symbol[:-4]
    return f"{base}/{quote}:{quote}"


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
    parsed_ts = pd.to_datetime(renamed["timestamp"], errors="coerce")
    if getattr(parsed_ts.dt, "tz", None) is None:
        # Local files are expected to be Beijing time in this project.
        renamed["timestamp"] = parsed_ts.dt.tz_localize(None)
    else:
        renamed["timestamp"] = parsed_ts.dt.tz_convert(BEIJING_TZ).dt.tz_localize(None)
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


def _naive_beijing_to_utc_ms(ts: pd.Timestamp) -> int:
    """Interpret naive timestamp as Beijing time, then convert to UTC milliseconds."""
    beijing_ts = pd.Timestamp(ts).tz_localize(BEIJING_TZ)
    return int(beijing_ts.tz_convert("UTC").timestamp() * 1000)


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

    # For live sync, always increment from the latest local bar.
    # Historical gaps are reported by validation, but should not force
    # the incremental fetch to rewind far into old history and miss the newest bars.
    last_ts = pd.Timestamp(existing.iloc[-1]["timestamp"])
    return _naive_beijing_to_utc_ms(last_ts) - _timeframe_milliseconds(timeframe)


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
        logger.debug("{} validation passed with {} continuous rows", label, report["tail_rows"])

    return continuous_df, report


def _sync_required_timeframes(project_root: Path, config: dict[str, Any], exchange_now: pd.Timestamp) -> None:
    timeframe_1h = config["basic"]["timeframe_1h"]
    timeframe_4h = config["basic"]["timeframe_4h"]
    timeframe_daily = config["basic"]["timeframe_daily"]
    sync_latest_ohlcv(project_root, config, timeframe_1h, exchange_now=exchange_now)
    time.sleep(0.2)
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
    timeframe_1h = config["basic"]["timeframe_1h"]
    timeframe_4h = config["basic"]["timeframe_4h"]
    timeframe_daily = config["basic"]["timeframe_daily"]
    if timeframe == timeframe_1h:
        explicit_key = "kline_1h_file"
    elif timeframe == timeframe_4h:
        explicit_key = "kline_4h_file"
    elif timeframe == timeframe_daily:
        explicit_key = "kline_1d_file"
    else:
        raise ValueError(f"Unsupported timeframe file mapping: {timeframe}")
    explicit = data_config.get(explicit_key)
    if explicit:
        explicit_path = project_root / explicit
        if explicit_path.exists():
            return explicit_path

    prefix = _symbol_to_file_prefix(config["basic"]["symbol"])
    if timeframe == timeframe_1h:
        filename = f"{prefix}_3year_{timeframe}.xlsx"
    elif timeframe == timeframe_4h:
        filename = f"{prefix}_3year_{timeframe}.xlsx"
    else:
        filename = f"{prefix}_3year_daily.xlsx"
    return project_root / data_config["realdata_dir"] / filename


def _build_exchange_with_health_check(config: dict[str, Any]) -> tuple[str, ccxt.Exchange]:
    symbol = _symbol_to_exchange_symbol(config["basic"]["symbol"])
    primary_exchange_name = str(config["basic"].get("platform", "binance")).lower()
    backup_exchange_names = _exchange_names(config)
    last_error: Exception | None = None

    for exchange_name in backup_exchange_names:
        try:
            exchange = _build_exchange(config, exchange_name=exchange_name)
            exchange.load_markets()
            exchange.market(symbol)
            exchange.fetch_time()
            if exchange_name != primary_exchange_name:
                logger.warning("primary exchange unavailable, fallback to backup source {}", exchange_name)
            else:
                logger.info("market data source health check passed: {}", exchange_name)
            return exchange_name, exchange
        except Exception as exc:
            last_error = exc
            logger.warning("market data source health check failed for {}: {}", exchange_name, exc)

    raise RuntimeError(f"all market data sources failed health check for {symbol}: {last_error}")


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
    frame["timestamp"] = (
        pd.to_datetime(frame["timestamp"], unit="ms", utc=True).dt.tz_convert(BEIJING_TZ).dt.tz_localize(None)
    )
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
        last_ts_ms = _naive_beijing_to_utc_ms(pd.Timestamp(batch.iloc[-1]["timestamp"]))
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


def _sync_latest_ohlcv_with_retry(
    project_root: Path,
    config: dict[str, Any],
    timeframe: str,
    exchange_now: pd.Timestamp,
    exchange: ccxt.Exchange,
) -> Path:
    max_attempts = int(config.get("data", {}).get("validation", {}).get("max_sync_attempts", 3))
    max_attempts = max(max_attempts, 1)
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("sync attempt {}/{} for {}", attempt, max_attempts, timeframe)
            return sync_latest_ohlcv(project_root, config, timeframe, exchange_now=exchange_now, exchange=exchange)
        except Exception as exc:
            last_error = exc
            logger.warning(
                "sync attempt {}/{} failed for {}: {}",
                attempt,
                max_attempts,
                timeframe,
                exc,
            )
            if attempt < max_attempts:
                time.sleep(0.5)

    raise RuntimeError(f"sync failed for {timeframe} after {max_attempts} attempts: {last_error}")


def sync_latest_ohlcv(
    project_root: Path,
    config: dict[str, Any],
    timeframe: str,
    exchange_now: pd.Timestamp,
    exchange: ccxt.Exchange,
) -> Path:
    path = _resolve_data_file(project_root, config, timeframe)
    existing = _load_table(path)
    symbol = _symbol_to_exchange_symbol(config["basic"]["symbol"])
    existing_rows = len(existing)

    try:
        logger.info("start sync {} {} -> {} (existing_rows={})", symbol, timeframe, path.name, existing_rows)
        incoming = _fetch_incremental_bars(exchange, symbol, timeframe, existing, exchange_now=exchange_now)
        incoming_rows = len(incoming)
        merged = _merge_incremental_data(existing, incoming)
        merged_rows = len(merged)
        if merged.empty:
            raise RuntimeError(
                f"No data available for {symbol} {timeframe}; "
                f"path={path} existing_rows={existing_rows} incoming_rows={incoming_rows} merged_rows={merged_rows}"
            )

        _save_table(merged, path)
        logger.info(
            "saved {} rows to {} (existing_rows={} incoming_rows={})",
            merged_rows,
            path.name,
            existing_rows,
            incoming_rows,
        )
        return path
    except Exception as exc:
        logger.error(
            "sync failed for {} {} -> {} | existing_rows={} | error_type={} | error={}",
            symbol,
            timeframe,
            path,
            existing_rows,
            type(exc).__name__,
            exc,
        )
        raise


def _latest_closed_bar_time_from_exchange(
    config: dict[str, Any],
    timeframe: str,
    exchange_now: pd.Timestamp,
    exchange: ccxt.Exchange,
) -> pd.Timestamp:
    symbol = _symbol_to_exchange_symbol(config["basic"]["symbol"])
    bars = _fetch_closed_bars(exchange, symbol, timeframe, exchange_now=exchange_now, since=None, limit=10)
    if bars.empty:
        raise RuntimeError(f"NOK! exchange has no closed {timeframe} bar for {symbol}")
    return pd.Timestamp(bars.iloc[-1]["timestamp"])


def _is_local_closed_bar_aligned(
    project_root: Path,
    config: dict[str, Any],
    timeframe: str,
    exchange_now: pd.Timestamp,
    exchange: ccxt.Exchange,
) -> bool:
    path = _resolve_data_file(project_root, config, timeframe)
    local_df = _load_table(path)
    exchange_latest = _latest_closed_bar_time_from_exchange(config, timeframe, exchange_now, exchange)

    if local_df.empty:
        logger.error(
            "NOK! local {} data file is empty, exchange latest closed bar is {}",
            timeframe,
            exchange_latest.isoformat(),
        )
        return False

    local_latest = pd.Timestamp(local_df.iloc[-1]["timestamp"])
    if local_latest != exchange_latest:
        logger.error(
            "NOK! local/exchange {} closed bar mismatch, local_latest={} exchange_latest={}",
            timeframe,
            local_latest.isoformat(),
            exchange_latest.isoformat(),
        )
        return False

    logger.info("local/exchange {} closed bar aligned: {}", timeframe, local_latest.isoformat())
    return True


def _alignment_failure_reason(
    project_root: Path,
    config: dict[str, Any],
    timeframe: str,
    exchange_now: pd.Timestamp,
    exchange: ccxt.Exchange,
) -> str:
    path = _resolve_data_file(project_root, config, timeframe)
    local_df = _load_table(path)
    exchange_latest = _latest_closed_bar_time_from_exchange(config, timeframe, exchange_now, exchange)

    if local_df.empty:
        return (
            f"{timeframe} local file is empty after sync; "
            f"path={path} exchange_latest={exchange_latest.isoformat()}"
        )

    local_latest = pd.Timestamp(local_df.iloc[-1]["timestamp"])
    row_count = len(local_df)
    delta_seconds = int((exchange_latest - local_latest).total_seconds())

    return (
        f"{timeframe} local/exchange latest closed bar mismatch after sync; "
        f"path={path} rows={row_count} "
        f"local_latest={local_latest.isoformat()} "
        f"exchange_latest={exchange_latest.isoformat()} "
        f"delta_seconds={delta_seconds}"
    )


def load_market_data(project_root: Path, config: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str, str]:
    def _load_once() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str, str]:
        timeframe_1h = config["basic"]["timeframe_1h"]
        timeframe_4h = config["basic"]["timeframe_4h"]
        timeframe_daily = config["basic"]["timeframe_daily"]
        validation_config = config["data"].get("validation", {})
        min_rows_1h = int(validation_config.get("min_rows_1h", 200))
        min_rows_4h = int(validation_config.get("min_rows_4h", 200))
        min_rows_1d = int(validation_config.get("min_rows_1d", 60))

        exchange_clock = fetch_exchange_clock(config)
        exchange_now = exchange_clock["exchange_now"]
        source_name, exchange = _build_exchange_with_health_check(config)
        logger.info("using market data source: {}", source_name)
        bar_1h_ok = _is_local_closed_bar_aligned(project_root, config, timeframe_1h, exchange_now, exchange)
        bar_4h_ok = _is_local_closed_bar_aligned(project_root, config, timeframe_4h, exchange_now, exchange)
        bar_1d_ok = _is_local_closed_bar_aligned(project_root, config, timeframe_daily, exchange_now, exchange)

        if not bar_1h_ok or not bar_4h_ok or not bar_1d_ok:
            logger.warning("local xlsx closed bar time is not aligned with exchange, start sync")
            _sync_latest_ohlcv_with_retry(project_root, config, timeframe_1h, exchange_now=exchange_now, exchange=exchange)
            time.sleep(0.2)
            _sync_latest_ohlcv_with_retry(project_root, config, timeframe_4h, exchange_now=exchange_now, exchange=exchange)
            time.sleep(0.2)
            _sync_latest_ohlcv_with_retry(project_root, config, timeframe_daily, exchange_now=exchange_now, exchange=exchange)

            if not _is_local_closed_bar_aligned(project_root, config, timeframe_1h, exchange_now, exchange):
                raise RuntimeError(_alignment_failure_reason(project_root, config, timeframe_1h, exchange_now, exchange))
            if not _is_local_closed_bar_aligned(project_root, config, timeframe_4h, exchange_now, exchange):
                raise RuntimeError(_alignment_failure_reason(project_root, config, timeframe_4h, exchange_now, exchange))
            if not _is_local_closed_bar_aligned(project_root, config, timeframe_daily, exchange_now, exchange):
                raise RuntimeError(_alignment_failure_reason(project_root, config, timeframe_daily, exchange_now, exchange))

        df_1h = _load_table(_resolve_data_file(project_root, config, timeframe_1h))
        df_4h = _load_table(_resolve_data_file(project_root, config, timeframe_4h))
        df_daily = _load_table(_resolve_data_file(project_root, config, timeframe_daily))

        if df_1h.empty or df_4h.empty or df_daily.empty:
            raise RuntimeError("NOK! market data files are empty after sync")

        df_1h, report_1h = _validate_market_frame(df_1h, timeframe_1h, min_rows_1h, "1h market data")
        df_4h, report_4h = _validate_market_frame(df_4h, timeframe_4h, min_rows_4h, "4h market data")
        df_daily, report_1d = _validate_market_frame(df_daily, timeframe_daily, min_rows_1d, "1d market data")

        logger.info(
            "market data ready: 1h_rows={} 4h_rows={} 1d_rows={} 1h_gaps={} 4h_gaps={} 1d_gaps={}",
            len(df_1h),
            len(df_4h),
            len(df_daily),
            report_1h["gap_count"],
            report_4h["gap_count"],
            report_1d["gap_count"],
        )

        latest_1h_bar_time = str(pd.to_datetime(df_1h.iloc[-1]["timestamp"]).isoformat())
        latest_4h_bar_time = str(pd.to_datetime(df_4h.iloc[-1]["timestamp"]).isoformat())
        return df_1h, df_4h, df_daily, latest_1h_bar_time, latest_4h_bar_time

    try:
        return _load_once()
    except Exception as first_error:
        logger.warning("market data load failed, trying clash failover with connectivity probe: {}", first_error)
        recovered = _try_clash_failover_and_wait_binance(project_root, config)
        if not recovered:
            raise
        return _load_once()


def seconds_until_next_timeframe_bar(timeframe: str, now: pd.Timestamp | None = None, offset_seconds: int = 5) -> int:
    current = now or pd.Timestamp.utcnow()
    current = current.tz_localize(None)
    if timeframe.endswith("h"):
        hours = _timeframe_hours(timeframe)
        next_bar_hour = ((current.hour // hours) + 1) * hours
        next_bar = current.normalize() + pd.Timedelta(hours=next_bar_hour)
        if next_bar_hour >= 24:
            next_bar = current.normalize() + pd.Timedelta(days=1)
    elif timeframe.endswith("d"):
        next_bar = current.normalize() + pd.Timedelta(days=1)
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    wait_seconds = max(int((next_bar - current).total_seconds()) + offset_seconds, offset_seconds)
    return wait_seconds


def seconds_until_next_timeframe_bar_by_exchange(
    config: dict[str, Any],
    timeframe: str,
    offset_seconds: int = 5,
) -> int:
    clock = fetch_exchange_clock(config)
    return seconds_until_next_timeframe_bar(timeframe, now=clock["exchange_now"], offset_seconds=offset_seconds)
