from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generic.Common import yml_reader
from generic.logger import init_report
from services.execution_service import execute_order
from services.short_4h_service import make_short_4h_decision, short_runtime_config


OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def _load_market_frames(project_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_1h = pd.read_excel(project_root / "realdatas" / "BTC_USDT_3year_1h.xlsx")
    df_4h = pd.read_excel(project_root / "realdatas" / "BTC_USDT_3year_4h.xlsx")
    df_daily = pd.read_excel(project_root / "realdatas" / "BTC_USDT_3year_daily.xlsx")
    return _prepare_frame(df_1h), _prepare_frame(df_4h), _prepare_frame(df_daily)


def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df[OHLCV_COLUMNS].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)
    return frame.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def _initial_status() -> dict[str, Any]:
    return {
        "service_status": "running",
        "position_status": "flat",
        "current_phase": "idle",
        "last_processed_4h_bar_time": "",
        "entry_price": 0.0,
        "entry_time": "",
        "last_action": "SKIP",
        "last_score": 0.0,
        "last_processed_1h_bar_time": "",
        "cooldown_count": 0,
    }


def _backtest_config(config: dict[str, Any]) -> dict[str, Any]:
    cfg = copy.deepcopy(config)
    cfg.setdefault("trade", {})["paper_trade"] = True
    cfg.setdefault("logging", {}).setdefault("console", {})["level"] = "INFO"
    cfg.setdefault("logging", {}).setdefault("file", {})["level"] = "INFO"
    return cfg


def backtest() -> None:
    project_root = PROJECT_ROOT
    config = _backtest_config(yml_reader(str(project_root / "config" / "config.yaml")))
    init_report(config["logging"], attempt=1, log_name="backtest")

    df_1h, df_4h, df_daily = _load_market_frames(project_root)
    result_df = df_4h.copy()
    result_df["buy_signal"] = pd.NA
    result_df["sell_signal"] = pd.NA

    status = _initial_status()
    runtime = short_runtime_config(config)
    logger.info("backtest starting with main-compatible short 4h loop")
    logger.info("short 4h config: {}", runtime)

    for i in range(1498, len(df_4h)):
        current_4h = df_4h.iloc[: i + 1].copy()
        current_time = pd.Timestamp(current_4h.iloc[-1]["timestamp"])
        current_1h = df_1h[df_1h["timestamp"] <= current_time].copy()
        current_daily = df_daily[df_daily["timestamp"] <= current_time].copy()

        try:
            decision = make_short_4h_decision(config, status, current_1h, current_4h, current_daily)
            execution_result = execute_order(config, status, decision, current_4h)
        except Exception as exc:
            logger.error("backtest failed at index={} time={}: {}", i, current_time.isoformat(), exc)
            continue

        signal_price = float(current_4h.iloc[-1]["close"])
        if decision["action"] == "SHORT":
            result_df.at[result_df.index[i], "buy_signal"] = signal_price
            logger.success(
                "short entry: index={} time={} price={} score={}",
                i,
                current_time.isoformat(),
                signal_price,
                decision["score"],
            )
        elif decision["action"] == "EXIT":
            result_df.at[result_df.index[i], "sell_signal"] = signal_price
            logger.success(
                "short exit: index={} time={} price={} reason={}",
                i,
                current_time.isoformat(),
                signal_price,
                decision.get("reason", "no_metric"),
            )

        status.update(execution_result["status_updates"])
        status["service_status"] = "running"
        status["last_processed_1h_bar_time"] = str(pd.Timestamp(current_1h.iloc[-1]["timestamp"]).isoformat())
        status["last_processed_4h_bar_time"] = str(current_time.isoformat())
        status["cooldown_count"] = int(decision.get("cooldown_count", status.get("cooldown_count", 0) or 0))

    save_path = project_root / "backtest_result_with_signals.xlsx"
    result_df.to_excel(save_path, index=False)
    logger.info("backtest result saved to: {}", save_path)


if __name__ == "__main__":
    backtest()
