# trade_agent

基于 4H K 线的交易代理项目。当前版本已经接通 Binance 行情同步、数据校验与自动修复、4H 定时调度、状态持久化、失败通知 mock 和文件日志。

## 会话记录

- 对话 ID: `id019d4e9b-a6b0-7673-a587-5c2c15a3f5d`

## 当前能力

- 读取本地 `realdatas/` 历史数据
- 每次打分前自动校验数据正确性和连续性
- 校验失败时自动从 Binance 同步并重试，最多 3 次
- 3 次仍失败时走 `push_message` mock 告警
- 以 Binance 交易所时间为准调度下一次 4H 执行
- 要求交易所时间与本机时间偏差不超过 60 秒
- 打分后把总分 `score` 和 `metrics` 打到日志
- 使用 `status.json` 维护服务状态和仓位状态
- 支持 `paper_trade` 和 `live` 模式

## 目录结构

- `app/main.py`
  程序入口。负责读取配置、初始化日志并进入循环调度。

- `app/orchestrator.py`
  主流程调度器。负责串联市场数据、决策、执行和状态回写。

- `services/market_data_service.py`
  市场数据服务。负责：
  - 读取本地数据文件
  - 从 Binance 增量同步数据
  - 自动修复尾部缺口
  - 校验 OHLCV、排序、重复、连续性和最小 K 线数量
  - 获取交易所时间并校验本地时间偏差

- `services/decision_service.py`
  负责把策略结果标准化为：
  - `action`
  - `score`
  - `metrics`

- `services/execution_service.py`
  执行层。
  - `paper_trade: true` 时只模拟执行
  - `paper_trade: false` 时尝试按 Binance API 下单

- `services/status_service.py`
  管理 `config/status.json` 的读写。

- `services/push_message.py`
  失败通知服务，当前为 mock 版本。

- `strategy/FourHour_long.py`
  4H Long 策略逻辑。

- `generic/logger.py`
  日志初始化模块。

- `config/config.yaml`
  主配置文件。

- `config/status.json`
  运行状态文件。

- `realdatas/`
  本地历史数据文件目录。

- `reports/`
  日志目录。

## 数据源与数据文件

默认数据文件：

- `realdatas/BTC_USDT_3year_4h.xlsx`
- `realdatas/BTC_USDT_3year_daily.xlsx`

如果切换标的，需要同步修改：

- `basic.symbol`
- `data.kline_4h_file`
- `data.kline_1d_file`

## 打分前的数据校验

每次进入策略打分前，系统都会先校验数据，不满足条件不会直接进入评分。

校验项包括：

- 是否包含 `timestamp/open/high/low/close/volume`
- 时间戳是否可解析
- 时间是否升序
- 是否有重复时间戳
- OHLC 是否合理
- 时间序列是否连续
- 最新连续区间是否满足最少 K 线数量

默认最少数量：

- `4h`: 200 根
- `1d`: 60 根

## 数据异常时的处理流程

如果本地数据校验失败：

1. 自动从 Binance 同步最新数据
2. 重新校验
3. 最多重试 3 次
4. 如果仍失败：
   - 记录 error 日志
   - 通过 `services/push_message.py` 发送 mock 失败通知
   - 本轮停止，不进入打分

## 交易所时间规则

4H 任务不是按本机时间触发，而是按 Binance 时间触发。

当前规则：

- 每轮运行前先获取 Binance 时间
- 校验本机和交易所时间偏差
- 偏差必须小于等于 60 秒
- 超过 60 秒，本轮视为异常并终止
- 下一次 4H bar 的等待时间按交易所时间计算

相关配置：

```yaml
runtime:
  run_forever: true
  run_delay_seconds: 5
  max_clock_diff_seconds: 60
```

## 评分日志

每次打分或决策后，日志都会打印：

- `decision`
- `score`
- `metrics`

示例：

```text
decision=BUY score=9.0 metrics=daily_above_ema200 | daily_ema200_rising | 4h_ema50_above_ema200
```

## 配置说明

### `basic`

- `platform`
  当前为 `binance`

- `symbol`
  当前为 `BTCUSDT`

- `timeframe_4h`
  当前为 `4h`

- `timeframe_daily`
  当前为 `1d`

- `buypoint`
  入场阈值

### `data`

- `realdata_dir`
  本地数据目录

- `kline_4h_file`
  4H 数据文件路径

- `kline_1d_file`
  1D 数据文件路径

- `sync_on_start`
  每轮开始前是否先同步交易所数据

- `validation.min_rows_4h`
  4H 最少连续 K 线数量

- `validation.min_rows_1d`
  1D 最少连续 K 线数量

- `validation.max_sync_attempts`
  校验失败后的最大同步尝试次数

### `trade`

- `paper_trade`
  - `true`：模拟执行
  - `false`：真实下单

- `quantity`
  下单数量

- `exit_freeze_bars`
  持仓后冻结的 4H K 线数量。
  在冻结期内不会执行 exit 逻辑，系统会直接返回 `HOLD`。
  当前默认值为 `3`。

### `network`

- `proxy`
  当前配置：
  `http://127.0.0.1:7897`

### `notify`

- `channel`
  当前为 `mock`

### `logging`

- `console.enabled`
  是否输出控制台日志

- `file.enabled`
  是否写入文件日志

- `file.path`
  当前为 `reports/`

## 运行方式

在项目根目录执行：

```bash
python app/main.py
```

默认流程：

1. 初始化日志
2. 启动先读取 `status.json`
3. 如果 `service_status == error`：
   - 直接记录 error 日志
   - 发送 `push_message` mock
   - 结束程序
4. 如果 `service_status != error`：
   - 第 1 轮强制走一次完整流程，不因为 `last_processed_4h_bar_time` 而跳过
   - 用于验证当前打分链路和状态写入是否正常
5. 从第 2 轮开始，再按交易所时间等待下一个 4H bar

## 实盘模式

如果要启用真实下单，需要先设置环境变量：

```powershell
$env:BINANCE_API_KEY="your_api_key"
$env:BINANCE_SECRET_KEY="your_secret_key"
```

并确保：

- `trade.paper_trade: false`
- 代理可用
- 本机时间和交易所时间偏差在允许范围内
- Binance API 权限正确

如果未设置密钥，程序会记录 warning，但不会真实下单。

## 日志

日志输出到：

- `reports/trade_agent_YYYY-MM-DD.log`

日志内容包括：

- 行情同步
- 数据校验
- 时钟校验
- 策略决策
- 总分与 metrics
- 执行结果
- 失败通知

## 当前已实现的失败通知

`services/push_message.py` 当前还是 mock 实现。

失败时会：

- 把 title / content 打到日志
- 返回 `status=mock_sent`

后续可以扩展成飞书、企业微信或 Telegram。

## 已知限制

- 当前数据文件仍是本地 `.xlsx`
- 如果手动打开 `.xlsx`，Windows 可能锁文件，影响同步写回
- `push_message` 目前不是正式通知通道
- 执行层目前是基础 market order 模型，没有补完整的风控和成交确认

## 后续建议

- 把 `push_message` 接成正式通知通道
- 给 live 下单补成交确认和异常回滚
- 增加 15m 执行层过滤
- 增加 Docker / 守护进程部署方式
