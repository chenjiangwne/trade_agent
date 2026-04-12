from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generic.Common import yml_reader
from generic.logger import init_report
from services.market_data_service import _load_table, _resolve_data_file
import strategy.FourHour_long as strategy_module
from strategy.FourHour_long import Res, StrategyResult, eval_exit, testsuite_result


def load_backtest_frames(project_root: Path, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    timeframe_4h = config["basic"]["timeframe_4h"]
    timeframe_daily = config["basic"]["timeframe_daily"]
    df_4h = _load_table(_resolve_data_file(project_root, config, timeframe_4h))
    df_daily = _load_table(_resolve_data_file(project_root, config, timeframe_daily))

    if df_4h.empty or df_daily.empty:
        raise RuntimeError("backtest data is empty")

    return df_4h, df_daily


def _disable_system_ready_check() -> None:
    strategy_module.is_system_ready = lambda df_4h, df_daily: False


def backtest() -> Path:
    project_root = PROJECT_ROOT
    config = yml_reader(str(project_root / "config" / "config.yaml"))
    init_report(config["logging"], attempt=1, log_name="backtest")
    logger.info("backtest starting")

    _disable_system_ready_check()
    df_4h, df_daily = load_backtest_frames(project_root, config)
    buypoint = float(config["basic"]["buypoint"])
    freeze_bars = int(config.get("trade", {}).get("exit_freeze_bars", 0))

    result_df = df_4h.copy()
    result_df["buy_signal"] = None
    result_df["sell_signal"] = None
    result_df["score"] = None
    result_df["metrics"] = None
    result_df["position_state"] = "flat"

    start_index = max(int(config.get("data", {}).get("validation", {}).get("min_rows_4h", 200)), 300)
    if len(result_df) <= start_index:
        raise RuntimeError(f"4h rows is not enough for backtest, rows={len(result_df)} start_index={start_index}")

    in_position = False
    entry_price = 0.0
    entry_time = None

    for index in range(start_index, len(result_df)):
        current_df_4h = df_4h.iloc[: index + 1].copy()
        current_bar = current_df_4h.iloc[-1]
        current_bar_time = pd.Timestamp(current_bar["timestamp"])
        current_close = float(current_bar["close"])
        current_df_daily = df_daily[df_daily["timestamp"] <= current_bar_time].copy()

        logger.info("---- start backtest iteration={} bar_time={} ----", index + 1, current_bar_time.isoformat())
        logger.debug("backtest scoring input 4h tail={}", _tail_records(current_df_4h, 6))
        logger.debug("backtest scoring input daily tail={}", _tail_records(current_df_daily, 6))

        if not in_position:
            result, total_score, metrics = testsuite_result(current_df_4h, current_df_daily)
            metric_text = _normalize_metrics(metrics)
            # result_df.at[result_df.index[index], "score"] = float(total_score)
            # result_df.at[result_df.index[index], "metrics"] = metric_text
            # result_df.at[result_df.index[index], "position_state"] = "flat"

            if result != Res["OK"]:
                logger.error(
                    "--- NOK! backtest scoring failed at index={} bar_time={} score={} metrics={} ---",
                    index + 1,
                    current_bar_time.isoformat(),
                    total_score,
                    metric_text,
                )
                raise RuntimeError(f"backtest scoring failed at index={index + 1}")

            if float(total_score) >= buypoint:
                in_position = True
                entry_price = current_close
                entry_time = current_bar_time
                result_df.at[result_df.index[index], "buy_signal"] = current_close
                result_df.at[result_df.index[index], "position_state"] = "long"
                logger.success(
                    "--- OK! index={} bar_time={} score={} >= buypoint={}, buy_signal={} metrics={} ---",
                    index + 1,
                    current_bar_time.isoformat(),
                    total_score,
                    buypoint,
                    current_close,
                    metric_text,
                )
            else:
                logger.warning(
                    "--- WAIT! index={} bar_time={} score={} < buypoint={}, no signal. metrics={} ---",
                    index + 1,
                    current_bar_time.isoformat(),
                    total_score,
                    buypoint,
                    metric_text,
                )
            continue

        result_df.at[result_df.index[index], "position_state"] = "long"
        bars_since_entry = _bars_since_entry(current_df_4h, entry_time)
        if freeze_bars > 0 and bars_since_entry <= freeze_bars:
            freeze_metric = f"exit frozen for first {freeze_bars} bars after entry"
            result_df.at[result_df.index[index], "score"] = 0.0
            result_df.at[result_df.index[index], "metrics"] = freeze_metric
            logger.warning(
                "--- FREEZE! index={} bar_time={} bars_since_entry={} <= exit_freeze_bars={}, skip exit logic ---",
                index + 1,
                current_bar_time.isoformat(),
                bars_since_entry,
                freeze_bars,
            )
            continue

        exit_result, exit_signal = eval_exit(current_df_4h, entry_price)
        exit_action = _read_exit_action(exit_signal)
        exit_metric = _read_exit_metric(exit_signal)
        result_df.at[result_df.index[index], "score"] = 0.0
        result_df.at[result_df.index[index], "metrics"] = exit_metric

        if exit_result == Res["OK"] and exit_action == StrategyResult.EXIT.value:
            result_df.at[result_df.index[index], "sell_signal"] = current_close
            result_df.at[result_df.index[index], "position_state"] = "flat"
            logger.success(
                "--- EXIT! index={} bar_time={} sell_signal={} metric={} ---",
                index + 1,
                current_bar_time.isoformat(),
                current_close,
                exit_metric,
            )
            in_position = False
            entry_price = 0.0
            entry_time = None
        else:
            logger.info(
                "--- HOLD! index={} bar_time={} metric={} ---",
                index + 1,
                current_bar_time.isoformat(),
                exit_metric,
            )

    output_path = project_root / "reports" / "backtest_result_with_signals.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_excel(output_path, index=False)
    buy_count = int(result_df["buy_signal"].notna().sum())
    sell_count = int(result_df["sell_signal"].notna().sum())
    logger.info("backtest completed: output={} buy_signals={} sell_signals={}", output_path, buy_count, sell_count)
    return output_path


def _read_exit_action(exit_signal: object) -> str:
    if exit_signal is None:
        return StrategyResult.WAIT.value
    strategy_result = getattr(exit_signal, "StrategyResult", None)
    return str(getattr(strategy_result, "value", StrategyResult.WAIT.value))


def _read_exit_metric(exit_signal: object) -> str:
    if exit_signal is None:
        return "no_exit_signal"
    return str(getattr(exit_signal, "metric", "no_exit_signal"))


def _bars_since_entry(df_4h: pd.DataFrame, entry_time: pd.Timestamp | None) -> int:
    if df_4h.empty or entry_time is None:
        return 0
    return int((pd.to_datetime(df_4h["timestamp"]) > pd.Timestamp(entry_time)).sum())


def _normalize_metrics(metrics: object) -> str:
    if metrics is None:
        return "no_metric"
    if isinstance(metrics, list):
        return " | ".join(str(item) for item in metrics) if metrics else "no_metric"
    return str(metrics)


def _tail_records(df: pd.DataFrame, rows: int) -> str:
    if df.empty:
        return "[]"
    columns = [column for column in ["timestamp", "open", "high", "low", "close", "volume"] if column in df.columns]
    preview = df[columns].tail(rows).copy()
    preview["timestamp"] = pd.to_datetime(preview["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    return preview.to_json(orient="records", force_ascii=False)


if __name__ == "__main__":
    backtest()
