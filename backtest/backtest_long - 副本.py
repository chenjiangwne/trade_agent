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
Res = {"OK": 0, "ERR": -1, "EXCEPTION": -2, "empty": 1, "position": 2}


@contextmanager
def test_environment():
    try:
        one_hour_path = "realdatas/BTC_USDT_3year_1h.xlsx"
        four_path = "realdatas/BTC_USDT_3year_4h.xlsx"
        daliy_path = "realdatas/BTC_USDT_3year_daily.xlsx"

        df_1h = pd.read_excel(one_hour_path)
        df_daily = pd.read_excel(daliy_path)
        df_4h = pd.read_excel(four_path)
        yield df_1h, df_4h, df_daily
    finally:
        pass


def _normalize_metrics(metrics):
    if metrics is None:
        return ""
    if isinstance(metrics, list):
        return " | ".join(str(item) for item in metrics)
    return str(metrics)


def _normalize_parameters(metrics):
    if isinstance(metrics, dict):
        metric_text = metrics.get("metric_str", "")
        return metric_text, metrics
    return _normalize_metrics(metrics), {"metric_str": _normalize_metrics(metrics)}


def _mock_initial_long_stop(entry_price: float, current_df_4h: pd.DataFrame) -> float:
    if current_df_4h is None or current_df_4h.empty:
        return float(entry_price * 0.99)
    recent_low = pd.to_numeric(current_df_4h["low"], errors="coerce").tail(20).min()
    if pd.isna(recent_low) or recent_low <= 0:
        return float(entry_price * 0.99)
    return float(min(entry_price * 0.99, recent_low * 0.995))


def backtest():
    with test_environment() as (df_1h, df_4h, df_daily):
        df_4h["buy_signal"] = None
        df_4h["sell_signal"] = None
        df_4h["sell_signal_type"] = None
        state = Res["empty"]

        config = yml_reader(str(project_root / "config" / "config.yaml"))
        buypoint = config["basic"]["buypoint"]
        buypoint_step = int(config.get("basic", {}).get("buypoint_step", 3))
        init_report(config["logging"], attempt=1, log_name="backtest_long")
        logger.info("backtest long starting")
        logger.debug(config)
        step = 1
        entry_price = None
        entry_index = None
        peak_rr = None
        last_entry_score = None
        initial_entry_price = None
        initial_stop_price = None

        for i in range(1498, len(df_4h), step):
            try:
                loop_logger = logger.bind(attempt=i + 1)
                loop_logger.info(f"----start excu strateg for {i + 1} interration----")
                current_df_4h = df_4h.iloc[: i + 1].copy()
                current_date = current_df_4h.iloc[-1]["timestamp"]
                current_4h_close_time = pd.to_datetime(current_date) + pd.Timedelta(hours=4)

                current_df_1h = df_1h[pd.to_datetime(df_1h["timestamp"]) <= current_4h_close_time].copy()
                current_df_daily = df_daily[pd.to_datetime(df_daily["timestamp"]) <= current_4h_close_time].copy()
                logger.debug(f"this is {i} after handle:1h is \n{current_df_1h}, 4h is \n{current_df_4h}, daily is \n{current_df_daily}")

                result, total_scores, metrics = testsuite_result(current_df_4h, current_df_daily)
                metric_text, parameters_log = _normalize_parameters(metrics)
                if result != Res["OK"]:
                    logger.debug(f"{result} total_scores is {total_scores}")
                    logger.error(f"---NOK! Attempt>>{i}<<success,The case failed to occur, parameters={parameters_log}, Please check log---")
                    break
                else:
                    logger.warning(f"---Index:{i + 1} | Score:{total_scores} | parameters={parameters_log} | metric_str={metric_text}  ---")

                required_entry_score = buypoint
                if state == Res["position"] and last_entry_score is not None:
                    required_entry_score = float(last_entry_score) + float(buypoint_step)

                if total_scores >= required_entry_score:
                    logger.info(
                        f"---✅ Index:{i + 1} | Score:{total_scores} | BUY Signal Recorded | entry_time={df_4h.iloc[i]['timestamp']} |"
                    )
                    if state != Res["position"]:
                        state = Res["position"]
                        entry_price = df_4h.iloc[i]["close"]
                        initial_entry_price = entry_price
                        entry_index = i
                        initial_stop_price = _mock_initial_long_stop(entry_price, current_df_4h)
                        peak_rr = 0.0
                        logger.success(f"---✅ Index:{i + 1} | 首次做多建仓 | 进入价格: {entry_price} |Initial Stop Price: {initial_stop_price}---")
                    else:
                        current_add_price = df_4h.iloc[i]["close"]
                        logger.success(f"---✅ Index:{i + 1} | 首次做多建仓趋势加仓 | 加仓价: {current_add_price} (首次进入价格:{initial_entry_price}) |Initial Stop Price: {initial_stop_price}---")

                    last_entry_score = float(total_scores)
                    df_4h.at[df_4h.index[i], "buy_signal"] = df_4h.iloc[i]["close"]
                    continue

                if config["backtest"]["eval_exit"]["enabled"]:
                    if state == Res["position"] and entry_price is not None:
                        current_k = df_4h.iloc[i]
                        if current_k["low"] <= initial_stop_price:
                            exit_price = initial_stop_price
                            metric = f"硬止损触发：最低价 {current_k['low']} 触及挂单位 {initial_stop_price}"
                            df_4h.at[df_4h.index[i], "sell_signal"] = exit_price
                            df_4h.at[df_4h.index[i], "sell_signal_type"] = "HARD_STOP"

                            logger.success(
                                f"---✅ Index:{i + 1} | Hardstop Exit Triggered: {metric} Entry Price: {initial_entry_price}| exit_time={df_4h.iloc[i]['timestamp']} | exit_price={exit_price} ---"
                            )
                            state, initial_entry_price, initial_stop_price, peak_rr, entry_price = Res["empty"], None, None, None, None
                            continue
                        else:
                            logger.info(f"---🔍 Index:{i + 1} | Evaluating Exit Conditions... ---")
                            current_context = df_4h.iloc[: i + 1].copy()
                            re, signal = eval_exit(current_context, initial_entry_price)

                            if re == Res["OK"]:
                                if signal.StrategyResult.value == "EXIT":
                                    exit_price = df_4h.iloc[i]["close"]
                                    df_4h.at[df_4h.index[i], "sell_signal"] = exit_price
                                    df_4h.at[df_4h.index[i], "sell_signal_type"] = "EXIT_SIGNAL"

                                    logger.success(
                                        f"---✅ Index:{i + 1} | Exit Triggered: {signal.metric} | exit_time={df_4h.iloc[i]['timestamp']} | exit_price={exit_price} ---"
                                    )

                                    state = Res["empty"]
                                    entry_price = None
                                    initial_entry_price = None
                                    initial_stop_price = None
                                    peak_rr = None
                            else:
                                logger.error(f"---NOK! Attempt>>{i}<<success,The Exit case failed to occur,Please check log---")
                                break
            except Exception as e:
                logger.error(f"Error at index {i + 1}: {e}")
                continue
            loop_logger.info(f"----ending excu strateg for {i + 1} interration----")

        save_path = "backtest_result_long_signals.xlsx"
        df_4h.to_excel(save_path, index=False)
        logger.info(f"回测结果已保存至: {save_path}")


if __name__ == "__main__":
    backtest()
