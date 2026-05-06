from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
logger.debug(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generic.Common import yml_reader
from generic.logger import init_report
from strategy.FourHour_long import Res, StrategyResult, eval_exit, testsuite_result


project_root = PROJECT_ROOT
STATE = {"empty": 1, "position": 2}


@contextmanager
def test_environment():
    try:
        four_path = "realdatas/BTC_USDT_3year_4h.xlsx"
        daily_path = "realdatas/BTC_USDT_3year_daily.xlsx"

        df_4h = pd.read_excel(four_path)
        df_daily = pd.read_excel(daily_path)
        yield df_4h, df_daily
    finally:
        pass


def _normalize_metrics(metrics: list[str] | str | None) -> str:
    if metrics is None:
        return "no_metric"
    if isinstance(metrics, list):
        return " | ".join(str(item) for item in metrics) if metrics else "no_metric"
    return str(metrics)


def _mock_initial_long_stop(entry_price: float, current_df_4h: pd.DataFrame) -> float:
    return float(entry_price * 0.99)


def backtest() -> None:
    with test_environment() as (df_4h, df_daily):
        df_4h["buy_signal"] = None
        df_4h["sell_signal"] = None
        df_4h["sell_signal_type"] = None
        state = STATE["empty"]

        config = yml_reader(str(project_root / "config" / "config.yaml"))
        buypoint = config["basic"]["buypoint"]
        init_report(config["logging"], attempt=1, log_name="backtest_long")
        logger.info("long backtest starting")
        logger.debug(config)

        step = 1
        entry_price = None
        entry_index = None
        initial_stop_price = None

        for i in range(199, len(df_4h), step):
            try:
                loop_logger = logger.bind(attempt=i + 1)
                loop_logger.info(f"----start long strategy for {i + 1} iteration----")

                current_df_4h = df_4h.iloc[: i + 1].copy()
                current_date = pd.to_datetime(current_df_4h.iloc[-1]["timestamp"])
                current_4h_close_time = current_date + pd.Timedelta(hours=4)
                current_df_daily = df_daily[pd.to_datetime(df_daily["timestamp"]) <= current_4h_close_time].copy()

                current_close = float(df_4h.iloc[i]["close"])

                if state == STATE["empty"]:
                    result, total_scores, metrics = testsuite_result(current_df_4h, current_df_daily)
                    if result != Res["OK"]:
                        logger.debug(f"{result} total_scores is {total_scores}")
                        logger.error(f"---NOK! Attempt>>{i}<<success,The case failed to occur,Please check log---")
                        break

                    metric_text = _normalize_metrics(metrics)
                    logger.warning(f"---Index:{i + 1} | Score:{total_scores} | {metric_text}---")
                    if float(total_scores) >= float(buypoint):
                        state = STATE["position"]
                        entry_price = current_close
                        entry_index = i
                        initial_stop_price = _mock_initial_long_stop(entry_price, current_df_4h)
                        df_4h.at[df_4h.index[i], "buy_signal"] = current_close
                        logger.success(
                            f"---✅ Index:{i + 1} | LONG Entry Triggered | entry_time={df_4h.iloc[i]['timestamp']} | entry_price={entry_price} | initial_stop_price={initial_stop_price} | reason={metric_text} ---"
                        )
                        continue
                elif config["backtest"]["eval_exit"]["enabled"] and entry_price is not None:
                    current_k = df_4h.iloc[i]
                    if initial_stop_price is not None and float(current_k["low"]) <= float(initial_stop_price):
                        exit_price = float(initial_stop_price)
                        metric = f"硬止损触发：最低价 {current_k['low']} 触及挂单位 {initial_stop_price}"
                        df_4h.at[df_4h.index[i], "sell_signal"] = exit_price
                        df_4h.at[df_4h.index[i], "sell_signal_type"] = "HARD_STOP"
                        logger.success(
                            f"---✅ Index:{i + 1} | LONG Hardstop Exit Triggered: {metric} | entry_index={entry_index} | exit_time={df_4h.iloc[i]['timestamp']} | exit_price={exit_price} ---"
                        )
                        state = STATE["empty"]
                        entry_price = None
                        entry_index = None
                        initial_stop_price = None
                        continue

                    result, signal = eval_exit(current_df_4h, entry_price)
                    if result != Res["OK"] or signal is None:
                        logger.error(f"---NOK! Attempt>>{i}<<success,The Exit case failed to occur,Please check log---")
                        break

                    if signal.StrategyResult == StrategyResult.EXIT:
                        exit_price = current_close
                        df_4h.at[df_4h.index[i], "sell_signal"] = exit_price
                        df_4h.at[df_4h.index[i], "sell_signal_type"] = signal.metric
                        logger.success(
                            f"---✅ Index:{i + 1} | LONG Exit Triggered: {signal.metric} | entry_index={entry_index} | exit_time={df_4h.iloc[i]['timestamp']} | exit_price={exit_price} ---"
                        )
                        state = STATE["empty"]
                        entry_price = None
                        entry_index = None
                        initial_stop_price = None
            except Exception as e:
                logger.error(f"Error at index {i + 1}: {e}")
                continue
            loop_logger.info(f"----ending long strategy for {i + 1} iteration----")

        save_path = "backtest_result_long_with_signals.xlsx"
        df_4h.to_excel(save_path, index=False)
        logger.info(f"long backtest result saved to: {save_path}")


if __name__ == "__main__":
    backtest()