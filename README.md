# trade_agent

基于 4H K 线的交易代理项目。当前版本已经接通真实历史数据文件、Binance 增量拉取、4 小时调度、状态持久化和文件日志。

## 会话记录

- 对话 ID: `id019d4e9b-a6b0-7673-a587-5c2c15a3f5d`

## 当前能力

- 读取本地真实数据文件作为基础数据源
- 通过 Binance 接口增量同步最新已闭合 `4h` 和 `1d` K 线
- 基于 `status.json` 维护运行状态，避免重复处理同一个 4H bar
- 按策略分数生成 `BUY / EXIT / HOLD / SKIP`
- 支持 `paper_trade` 和 `live` 两种执行模式
- 日志自动落到 `reports/`
- 支持按 4 小时闭合时间持续运行

## 项目结构

- `app/main.py`
  程序入口。读取配置、初始化日志，并按配置执行单轮或循环调度。

- `app/orchestrator.py`
  主流程调度器。串联状态读取、市场数据同步、策略决策、执行和状态回写。

- `services/status_service.py`
  统一管理 `config/status.json` 的读写。

- `services/market_data_service.py`
  负责市场数据加载与同步。
  当前逻辑：
  1. 优先读取 `realdatas/` 里的真实历史文件
  2. 通过 Binance 增量获取最新已闭合 bar
  3. 合并、去重、回写到本地数据文件
  4. 计算下一个 4H bar 的运行时间

- `services/decision_service.py`
  调用 `strategy/FourHour_long.py` 的策略逻辑，生成标准化交易动作。

- `services/execution_service.py`
  根据动作执行交易。
  当前行为：
  - `paper_trade: true` 时只更新状态，不下单
  - `paper_trade: false` 时尝试读取环境变量中的 Binance API 密钥下单

- `strategy/FourHour_long.py`
  4H Long 策略实现，包含入场评分和离场判断。

- `generic/Common.py`
  通用配置读取和 traceback 工具。

- `generic/logger.py`
  日志初始化，支持控制台和文件输出。

- `config/config.yaml`
  主配置文件。

- `config/status.json`
  运行状态文件。

- `realdatas/`
  本地历史行情文件目录。

- `reports/`
  日志输出目录。

## 数据文件

当前默认使用以下文件：

- `realdatas/BTC_USDT_3year_4h.xlsx`
- `realdatas/BTC_USDT_3year_daily.xlsx`

系统启动后会：

1. 读取本地 `.xlsx` 文件
2. 用 Binance 最新已闭合 bar 增量更新
3. 把合并后的数据写回原文件

如果你切换交易标的，需要同步修改 `config/config.yaml` 里的 `basic.symbol` 和数据文件路径。

## 配置说明

### `basic`

- `platform`: 当前为 `binance`
- `symbol`: 当前为 `BTCUSDT`
- `timeframe_4h`: 4 小时级别
- `timeframe_daily`: 日线级别
- `buypoint`: 入场阈值

### `data`

- `realdata_dir`: 本地数据目录
- `kline_4h_file`: 4H 数据文件路径
- `kline_1d_file`: 日线数据文件路径
- `sync_on_start`: 启动时是否先同步最新数据

### `trade`

- `paper_trade: true`
  只模拟执行，不真实下单

- `paper_trade: false`
  启用真实下单逻辑，但前提是你已经提供 Binance API 环境变量

- `quantity`
  下单数量

### `network`

- `proxy`
  当前配置为：
  `http://127.0.0.1:7897`

如果代理不可用，Binance 请求会失败。

### `runtime`

- `run_forever`
  `true` 表示程序会持续运行，等待每个新的 4H bar

- `run_delay_seconds`
  到达下一个 4H 边界后额外延迟几秒执行，避免拿到未完全闭合的 bar

### `logging`

- `console.enabled`
  是否输出到控制台

- `file.enabled`
  是否写入日志文件

- `file.path`
  日志目录，当前为 `reports/`

## 状态文件

`config/status.json` 是整个系统的单一状态源，包含：

- `service_status`
- `position_status`
- `current_phase`
- `last_processed_4h_bar_time`
- `entry_price`
- `entry_time`
- `last_action`
- `last_score`

系统每轮运行都会先读取状态，再根据最新 bar 结果更新。

## 运行方式

在项目根目录执行：

```bash
python app/main.py
```

默认行为：

- 启动时同步一次最新 4H / 1D 数据
- 运行一轮决策
- 如果 `runtime.run_forever: true`，则等待下一个 4H bar 再继续运行

## 实盘模式说明

当前项目已经具备 live 模式入口，但不会硬编码密钥。

如果要启用真实下单，需要先设置环境变量：

```powershell
$env:BINANCE_API_KEY="your_api_key"
$env:BINANCE_SECRET_KEY="your_secret_key"
```

然后确保：

- `config.yaml` 中 `trade.paper_trade: false`
- 代理可用
- Binance 账户和 API 权限正确

否则程序会记录 live 模式请求，但跳过真实下单。

## 日志

日志会写到：

- `reports/trade_agent_YYYY-MM-DD.log`

日志中会记录：

- 数据同步
- 策略决策
- 状态变化
- live / paper 执行情况
- 错误信息

## 当前已验证

已验证通过的链路：

- 通过代理访问 Binance
- 增量拉取最新 4H / 1D 数据
- 把数据写回 `.xlsx`
- 读取已有状态并执行一轮决策
- 把日志写入 `reports/`

## 已知限制

- 当前策略文件存在部分历史中文注释编码残留，但不影响运行
- 如果 Binance 代理不稳定，增量同步可能超时
- live 模式是否真实下单，取决于环境变量中的密钥是否已设置
- 当前执行层只接了基础 market order 流程，没有做更复杂的风控和成交校验

## 下一步建议

- 增加启动参数，支持单轮运行和后台常驻运行切换
- 增加异常重试和网络超时恢复
- 增加订单查询与成交确认
- 增加 15m 执行优化逻辑
- 增加 Docker 部署配置
