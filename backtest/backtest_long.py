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

from generic.Common import calc_atr, find_recent_swing_low, get_directional_basic_value, yml_reader
from generic.logger import init_report
from strategy.FourHour_long import Res, StrategyResult, eval_exit, testsuite_result, calc_long_performance

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


def backtest():
    with test_environment() as (df_1h, df_4h, df_daily):
        df_4h["buy_signal"] = None
        df_4h["sell_signal"] = None
        df_4h["sell_signal_type"] = None
        state = Res["empty"]

        config = yml_reader(str(project_root / "config" / "config.yaml"))
        buypoint = float(get_directional_basic_value(config, "buypoint", "long", 0))
        buypoint_step = float(get_directional_basic_value(config, "buypoint_step", "long", 3))
        init_report(config["logging"], attempt=1, log_name="backtest")
        logger.info("backtest starting")
        logger.debug(config)
        step = 1
        entry_price = None
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
                result, total_scores, parameters = testsuite_result(current_df_1h, current_df_4h, current_df_daily)
                metrics = parameters.get("metric_str", "") if isinstance(parameters, dict) else parameters
                parameters_log = parameters if isinstance(parameters, dict) else {"metric_str": parameters}
                if result != Res["OK"]:
                    logger.debug(f"{result} total_scores is {total_scores}")
                    logger.error(f"---NOK! Attempt>>{i}<<success,The case failed to occur, parameters={parameters_log}, Please check log---")
                    break
                else:
                    logger.warning(f"---Index:{i + 1} | Score:{total_scores} | parameters={parameters_log} | metric_str={metrics}  ---")
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
                        atr_4h = calc_atr(current_df_4h, period=14).iloc[-1]
                        recent_low = find_recent_swing_low(current_df_1h, lookback=20)
                        struct_stop = recent_low - 0.3 * atr_4h if not pd.isna(recent_low) else entry_price * 0.99
                        vol_stop = entry_price - 1.5 * atr_4h
                        initial_stop_price = max(struct_stop, vol_stop, entry_price * 0.99)

                        logger.debug(f"Initial Entry Price: {initial_entry_price}, ATR: {atr_4h}, Recent Low: {recent_low}, Initial Stop Price: {initial_stop_price}")
                        peak_rr = 0.0
                        logger.success(f"---✅ Index:{i + 1} | 首次做多建仓 | 进入价格: {entry_price} |Initial Stop Price: {initial_stop_price}---")
                    else:
                        current_add_price = df_4h.iloc[i]["close"]
                        logger.success(f"---✅ Index:{i + 1} | 首次做多建仓趋势加仓 | 加仓价: {current_add_price} (首次进入价格:{initial_entry_price}) |Initial Stop Price: {initial_stop_price}---")

                    last_entry_score = float(total_scores)
                    df_4h.at[df_4h.index[i], "buy_signal"] = df_4h.iloc[i]["close"]
                    continue
                if config["backtest"]["eval_exit"]["enabled"] == True:
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
                            state, initial_entry_price, initial_stop_price, peak_rr, entry_price, last_entry_score = Res["empty"], None, None, None, None, None
                            continue

                        else:
                            logger.info(f"---🔍 Index:{i + 1} | Evaluating Exit Conditions... ---")
                            current_context = df_4h.iloc[: i + 1].copy()
                            current_context_close_time = pd.to_datetime(current_context.iloc[-1]["timestamp"]) + pd.Timedelta(hours=4)
                            current_context_1h = df_1h[pd.to_datetime(df_1h["timestamp"]) <= current_context_close_time].copy()
                            current_p = current_context.iloc[-1]["close"]

                            performance = calc_long_performance(
                                entry_price=initial_entry_price,
                                current_price=current_p,
                                stop_loss_price=initial_stop_price,
                            )

                            current_rr = performance["rr"]
                            current_return_pct = performance["return_pct"]

                            if current_rr is None:
                                current_rr = 0.0

                            peak_rr = max(peak_rr, current_rr)

                            re, signal = eval_exit(
                                df_1h=current_context_1h,
                                df_4h=current_context,
                                current_price=current_p,
                                initial_stop=initial_stop_price,
                                current_rr=current_rr,
                                peak_rr=peak_rr,
                                return_pct=current_return_pct,
                            )

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
                                    last_entry_score = None
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
