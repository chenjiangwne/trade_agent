# trade_agent

BTC/USDT 策略运行与回测项目。当前主程序用于实盘/模拟盘轮询，回测用于生成 `backtest_result_with_signals.xlsx` 和交互式 HTML 报告。

## 当前策略语义

- 当前配置默认走做空：`strategy.short: true`，`strategy.long: false`。
- `main` 每 1h 唤醒一次，用于同步和校验数据。
- 做空策略只在新 4h K 线出现时触发决策，使用 `last_processed_4h_bar_time` 防止重复处理。
- 1h 数据是做空策略的辅助输入，不是独立入场触发器。
- 如果 4h 条件不满足，1h 信号再好也不会单独开仓。
- 做空回测和 main 共用 `services.short_4h_service.make_short_4h_decision(...)`。

## 数据文件

默认读取本地数据：

- `realdatas/BTC_USDT_3year_1h.xlsx`
- `realdatas/BTC_USDT_3year_4h.xlsx`
- `realdatas/BTC_USDT_3year_daily.xlsx`

数据同步失败会重试，失败原因会写入日志并推送异常消息。

## 关键配置

配置文件：`config/config.yaml`

```yaml
strategy:
  short: true
  long: false
  short_config:
    buypoint: 30
  long_config:
    buypoint: 60

backtest:
  active_side: short
  short:
    buypoint: 30
    cooldown_count: 1
    eval_exit:
      enabled: true
  long:
    buypoint: 60
    cooldown_count: 3
    eval_exit:
      enabled: true
```

说明：

- `backtest.short.cooldown_count` 是做空触发后的冻结 K 线数量配置。
- `status.cooldown_count` 是运行时剩余冻结 K 线数量，用于 main 重启后保持状态。
- `notify.channel: wecom` 时通过企业微信机器人推送信号、异常和心跳。

## main 运行逻辑

入口：

```powershell
python app\main.py
```

主流程：

1. 读取 `config/config.yaml` 和 `config/status.json`。
2. 同步并校验 1h、4h、daily 数据。
3. 每 1h 唤醒一次。
4. 如果当前没有新的 4h K 线，short 策略跳过。
5. 如果出现新的 4h K 线，调用做空决策。
6. 命中入场或出场信号时推送企业微信消息，并写入 `reports/trade_signals.csv`。
7. 每 30 分钟输出一次 heartbeat 日志并推送心跳消息。

## 做空回测

运行：

```powershell
python backtest\backtest.py
python backtest\generate_short_html.py
```

输出：

- `backtest_result_with_signals.xlsx`
- `strategy_backtest_dashboard.html`

当前做空回测结果：

- 总交易次数：88
- 胜率：47.73%
- 累计净收益率：+61.17%
- 总绝对利润：56966.09
- 盈亏比：2.03
- 最大回撤：-34.54%

注意：回测外层直接按 4h K 线循环；main 外层按 1h 唤醒，但只有新 4h K 线才真正执行 short 决策。对当前 4h 做空策略来说，实际决策点一致。

## 重要文件

- `app/main.py`：程序入口，负责循环调度和 heartbeat。
- `app/orchestrator.py`：加载数据、调用决策、执行订单、推送信号、写 CSV。
- `services/short_4h_service.py`：做空 4h 决策服务，main 和 backtest 共用。
- `services/decision_service.py`：原通用决策服务，主要用于非 short 路径。
- `services/execution_service.py`：模拟/实盘执行层。
- `services/market_data_service.py`：本地数据读取、同步、校验。
- `services/push_message.py`：企业微信/日志通知。
- `services/status_service.py`：读写 `config/status.json`。
- `strategy/FourHour_short.py`：做空策略核心。
- `backtest/backtest.py`：做空回测入口。
- `backtest/generate_short_html.py`：生成做空回测 HTML。

## 日志和信号记录

- 每次运行会在 `reports/` 下生成独立日志文件。
- 入场/出场信号会写入 `reports/trade_signals.csv`。
- 企业微信推送包括入场价格、出场价格、当前收盘价、信号时间、score 和 reason。

## 代理和网络

`config.yaml` 中：

```yaml
network:
  proxy: ""
```

如果本机直连 Binance 不通，可以改成代理地址，例如：

```yaml
network:
  proxy: "http://127.0.0.1:7897"
```

也可以使用项目根目录下的 bat 文件按直连或代理方式运行。

## 已知限制

- 当前做空实盘入口只推送信号；spot 环境不会真正开空单。
- 数据仍使用本地 `.xlsx` 文件，手动打开 Excel 可能导致写入失败。
- 如果要让 1h 独立触发提前入场，需要新增策略规则，否则会和当前 88 次回测不一致。
