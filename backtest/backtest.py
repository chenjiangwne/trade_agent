from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
logger.debug(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generic.Common import yml_reader
from generic.logger import init_report
from services.market_data_service import _load_table, _resolve_data_file
# import strategy.FourHour_long as strategy_module
from strategy.FourHour_short import Res, StrategyResult, eval_exit, testsuite_result
# from strategy.FourHour_long import Res, StrategyResult, eval_exit, testsuite_result
from contextlib import contextmanager

project_root = PROJECT_ROOT
Res = {"OK": 0, 'ERR': -1,"EXCEPTION":-2,"empty": 1, 'position': 2}
@contextmanager
def test_environment():
    """
    """

    try:
        one_hour_path = "realdatas/BTC_USDT_3year_1h.xlsx"
        four_path = "realdatas/BTC_USDT_3year_4h.xlsx"
        daliy_path = "realdatas/BTC_USDT_3year_daily.xlsx"
        minute_path = "realdatas/BTC_USDT_3year_15m.xlsx"

        df_1h = pd.read_excel(one_hour_path)
        df_daily=pd.read_excel(daliy_path)
        df_4h = pd.read_excel(four_path)
        # df_15m = pd.read_excel(minute_path)
        yield df_1h, df_4h, df_daily
       

    finally:
        pass
def backtest():
    with test_environment() as (df_1h, df_4h, df_daily):
        # 1. 预初始化 buy_signal 列，填充为 None
        df_4h['buy_signal'] = None 
        state=Res['empty']
        

        config = yml_reader(str(project_root / "config" / "config.yaml"))
        buypoint = config['basic']['buypoint']
        init_report(config["logging"], attempt=1, log_name="backtest")
        logger.info("backtest starting")
        logger.debug(config)
        step = 1 
        cooldown_count = 0
        for i in range(1498, len(df_4h), step):
            try:
                #冻结周期
                if cooldown_count > 0:
                    logger.info(f"--- 🧊 Index:{i+1} Buy lock up period (remaining {cooldown_count} cvcles) ---")
                    cooldown_count -= 1
                    continue
                loop_logger = logger.bind(attempt=i+1)
                loop_logger.info(f"----start excu strateg for {i+1} interration----")
                # 模拟当前时刻：截取截止到第 i 行的数据
                current_df_4h = df_4h.iloc[:i+1].copy() # copy 防止 SettingWithCopyWarning
                current_date = current_df_4h.iloc[-1]['timestamp']

                current_df_1h = df_1h[df_1h['timestamp'] <= current_date].copy()
                current_df_daily = df_daily[df_daily['timestamp'] <= current_date].copy()
                logger.debug(f"this is {i} after handle:1h is \n{current_df_1h}, 4h is \n{current_df_4h}, daily is \n{current_df_daily}")
                # 执行打分：务必传入切片！
                result, total_scores,metrics = testsuite_result(current_df_1h, current_df_4h, current_df_daily)
                if result!=Res['OK']:
                    logger.debug(f"{result} total_scores is {total_scores}")
                    logger.error(f"---NOK! Attempt>>{i}<<success,The case failed to occur,Please check log---")
                    break
                else:
                    logger.warning(f"---Index:{i+1} | Score:{total_scores} | {metrics}  ---")
                if total_scores >= buypoint:
                    state=Res['position']
                    # 
                    df_4h.at[df_4h.index[i], 'buy_signal'] = df_4h.iloc[i]['close']
                    logger.success(f"---✅ Index:{i+1} | Score:{total_scores} | BUY Signal Recorded |Start Buy lock up  ---")
                    cooldown_count=config['basic']['cooldown_count']
                    # if config['backtest']["eval_execution_15m"]["enabled"] ==True:
                    #     current_15m_exec = df_15m[
                    #         (df_15m['timestamp'] <= current_date + pd.Timedelta(minutes=15))
                    #     ].copy()
                    #     logger.debug(f"df_15m is:\n{current_15m_exec}")
                    #     if not current_15m_exec.empty:
                    #         exec_price = current_15m_exec.iloc[-1]['close']
                    #         logger.debug(f"4H close time: {current_date} | Observation 15m Execution Window: {current_15m_exec.iloc[-1]['timestamp']}")
                    #         result, action = eval_execution_15m(current_15m_exec)
                    #         if action.StrategyResult.value=='LONG':
                    #             # df_4h.at[df_4h.index[i], 'buy_signal'] = df_4h.iloc[i]['close']
                    #             # logger.success(f"---✅ Index:{i+1} | Score:{total_scores} | BUY Signal Recorded |Start Buy lock up  ---")
                    #             # cooldown_count=config['basic']['cooldown_count']
                    #             logger.success(f"---✅ Index:{i+1} | 15m action result:{action}  | LONG Signal Recorded ---")
                    #         else:
                    #             logger.error(f"NOK! action:{action} result incorrect!")
                    
                    continue 
                if config['backtest']["eval_exit"]["enabled"] ==True:
                    if state == Res['position']:

                        if i + 1 < len(df_4h):
                            current_context = df_4h.iloc[:i+2].copy() 
                            current_context_1h = df_1h[df_1h['timestamp'] <= current_context.iloc[-1]['timestamp']].copy()
                            buy_price = df_4h.iloc[i]['close'] 
                            #exit
                            re, signal = eval_exit(current_context_1h, current_context, entry_price=buy_price)
                            logger.debug(f'signal.StrategyResult:{signal}')
                            if re == Res['OK'] :
                                if signal.StrategyResult.value == 'EXIT':
                                    df_4h.at[df_4h.index[i+1], 'sell_signal'] = df_4h.iloc[i+1]['close']
                                    state = Res['empty'] 
                                    logger.success(f"---✅ Index:{i+1} | Exit Triggered: {signal.metric} ---")
                                else:
                                    pass
                            else:
                                logger.error(f"---NOK! Attempt>>{i}<<success,The Exit case failed to occur,Please check log---")
                                break
                    
            except Exception as e:
                logger.error(f"Error at index {i+1}: {e}")
                # get_traceback()
                continue
            loop_logger.info(f"----ending excu strateg for {i+1} interration----")
        
        
        save_path = "backtest_result_with_signals.xlsx"
        df_4h.to_excel(save_path, index=False)
        logger.info(f"回测结果已保存至: {save_path}")



if __name__ == "__main__":
    backtest()
