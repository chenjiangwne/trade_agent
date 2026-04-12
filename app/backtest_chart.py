from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

REPORTS_DIR = PROJECT_ROOT / "reports"
SOURCE_XLSX = REPORTS_DIR / "backtest_result_with_signals.xlsx"
OUTPUT_HTML = REPORTS_DIR / "backtest_kline.html"


def build_chart() -> Path:
    if not SOURCE_XLSX.exists():
        raise FileNotFoundError(f"backtest result file not found: {SOURCE_XLSX}")

    df = pd.read_excel(SOURCE_XLSX)
    if df.empty:
        raise RuntimeError("backtest result file is empty")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    categories = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
    kline_data = df[["open", "close", "low", "high"]].round(6).values.tolist()

    buy_points = []
    sell_points = []
    for index, row in df.iterrows():
        low_price = float(row["low"])
        high_price = float(row["high"])
        if pd.notna(row.get("buy_signal")):
            buy_points.append(
                {
                    "name": "BUY",
                    "value": [categories[index], low_price * 0.992, float(row["buy_signal"])],
                }
            )
        if pd.notna(row.get("sell_signal")):
            sell_points.append(
                {
                    "name": "SELL",
                    "value": [categories[index], high_price * 1.008, float(row["sell_signal"])],
                }
            )

    summary = _build_summary(df)
    title = f"Backtest Kline - BUY {summary['buy_count']} / SELL {summary['sell_count']}"
    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{title}</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root {{
      --bg: #0b1020;
      --panel: #111936;
      --grid: #22305f;
      --text: #d7def7;
      --muted: #8fa1d4;
      --up: #18b26b;
      --down: #e04f5f;
      --buy: #19c37d;
      --sell: #ff5f6d;
      --card: rgba(255,255,255,0.04);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: radial-gradient(circle at top, #16234d 0%, var(--bg) 55%); color: var(--text); font-family: Consolas, "Microsoft YaHei", monospace; }}
    .wrap {{ width: min(96vw, 1800px); margin: 0 auto; padding: 20px 16px 28px; }}
    .head {{ margin-bottom: 12px; }}
    .title {{ font-size: 24px; font-weight: 700; }}
    .meta {{ color: var(--muted); margin-top: 6px; font-size: 13px; }}
    .summary {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; margin: 18px 0; }}
    .card {{ background: var(--card); border: 1px solid rgba(143, 161, 212, 0.18); border-radius: 14px; padding: 14px 16px; box-shadow: 0 12px 30px rgba(0,0,0,0.20); }}
    .label {{ color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
    .value {{ font-size: 24px; font-weight: 700; }}
    .value.small {{ font-size: 20px; }}
    .panel {{ background: rgba(17, 25, 54, 0.88); border: 1px solid rgba(143, 161, 212, 0.18); border-radius: 18px; overflow: hidden; box-shadow: 0 20px 60px rgba(0,0,0,0.35); }}
    #chart {{ width: 100%; height: 880px; }}
    @media (max-width: 1100px) {{ .summary {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
    @media (max-width: 640px) {{ .summary {{ grid-template-columns: 1fr; }} #chart {{ height: 680px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <div class="title">{title}</div>
      <div class="meta">数据源: {SOURCE_XLSX.name} | 时间范围: {categories[0]} 至 {categories[-1]}</div>
    </div>

    <div class="summary">
      <div class="card"><div class="label">胜率</div><div class="value">{summary['win_rate']}</div></div>
      <div class="card"><div class="label">收益</div><div class="value">{summary['total_return']}</div></div>
      <div class="card"><div class="label">盈亏比</div><div class="value">{summary['profit_loss_ratio']}</div></div>
      <div class="card"><div class="label">回撤</div><div class="value">{summary['max_drawdown']}</div></div>
      <div class="card"><div class="label">交易次数</div><div class="value small">{summary['trade_count']}</div></div>
    </div>

    <div class="panel">
      <div id="chart"></div>
    </div>
  </div>
  <script>
    const categories = {json.dumps(categories, ensure_ascii=False)};
    const klineData = {json.dumps(kline_data, ensure_ascii=False)};
    const buyPoints = {json.dumps(buy_points, ensure_ascii=False)};
    const sellPoints = {json.dumps(sell_points, ensure_ascii=False)};

    const chart = echarts.init(document.getElementById('chart'));
    const option = {{
      backgroundColor: 'transparent',
      animation: false,
      legend: {{ top: 12, textStyle: {{ color: '#d7def7' }} }},
      tooltip: {{
        trigger: 'axis',
        axisPointer: {{ type: 'cross' }},
        backgroundColor: 'rgba(12,18,38,0.95)',
        borderColor: '#31457f',
        textStyle: {{ color: '#e7ecff' }},
        formatter: params => {{
          const lines = [params[0].axisValueLabel];
          params.forEach(item => {{
            if (item.seriesName === 'K线') {{
              const d = item.data;
              lines.push(`K线 O:${{d[0]}} C:${{d[1]}} L:${{d[2]}} H:${{d[3]}}`);
            }} else if (Array.isArray(item.data?.value)) {{
              lines.push(`${{item.seriesName}} 信号: ${{item.data.value[2]}}`);
            }}
          }});
          return lines.join('<br/>');
        }}
      }},
      grid: {{ left: 56, right: 28, top: 56, bottom: 92 }},
      xAxis: {{
        type: 'category',
        data: categories,
        boundaryGap: true,
        axisLine: {{ lineStyle: {{ color: '#4760a8' }} }},
        axisLabel: {{ color: '#9db0e8' }},
        splitLine: {{ show: false }}
      }},
      yAxis: {{
        scale: true,
        axisLine: {{ show: false }},
        axisLabel: {{ color: '#9db0e8' }},
        splitLine: {{ lineStyle: {{ color: 'rgba(71,96,168,0.18)' }} }}
      }},
      dataZoom: [
        {{ type: 'inside', xAxisIndex: [0], start: 82, end: 100 }},
        {{ type: 'slider', xAxisIndex: [0], bottom: 26, height: 28, borderColor: '#31457f', backgroundColor: '#10182f', fillerColor: 'rgba(58, 94, 202, 0.35)', textStyle: {{ color: '#9db0e8' }}, start: 82, end: 100 }}
      ],
      series: [
        {{
          name: 'K线',
          type: 'candlestick',
          data: klineData,
          z: 2,
          itemStyle: {{
            color: '#18b26b',
            color0: '#e04f5f',
            borderColor: '#18b26b',
            borderColor0: '#e04f5f'
          }}
        }},
        {{
          name: 'BUY',
          type: 'scatter',
          data: buyPoints,
          symbol: 'triangle',
          symbolSize: 24,
          encode: {{ x: 0, y: 1 }},
          z: 8,
          itemStyle: {{ color: '#19c37d', borderColor: '#eafff4', borderWidth: 2, shadowBlur: 18, shadowColor: 'rgba(25,195,125,0.55)' }},
          label: {{ show: true, formatter: 'BUY', position: 'bottom', distance: 6, color: '#eafff4', fontSize: 12, fontWeight: 'bold', backgroundColor: 'rgba(25,195,125,0.18)', padding: [3, 6], borderRadius: 4 }}
        }},
        {{
          name: 'SELL',
          type: 'scatter',
          data: sellPoints,
          symbol: 'diamond',
          symbolSize: 20,
          encode: {{ x: 0, y: 1 }},
          z: 8,
          itemStyle: {{ color: '#ff5f6d', borderColor: '#fff1f3', borderWidth: 2, shadowBlur: 18, shadowColor: 'rgba(255,95,109,0.55)' }},
          label: {{ show: true, formatter: 'SELL', position: 'top', distance: 6, color: '#fff1f3', fontSize: 12, fontWeight: 'bold', backgroundColor: 'rgba(255,95,109,0.18)', padding: [3, 6], borderRadius: 4 }}
        }}
      ]
    }};
    chart.setOption(option);
    window.addEventListener('resize', () => chart.resize());
  </script>
</body>
</html>
'''

    OUTPUT_HTML.write_text(html, encoding="utf-8")
    return OUTPUT_HTML


def _build_summary(df: pd.DataFrame) -> dict[str, str | int]:
    trades = []
    entry_price = None
    equity = 1.0
    equity_curve = [equity]
    wins = 0
    losses = 0
    gross_profit = 0.0
    gross_loss = 0.0

    for _, row in df.iterrows():
        buy_signal = row.get("buy_signal")
        sell_signal = row.get("sell_signal")

        if pd.notna(buy_signal) and entry_price is None:
            entry_price = float(buy_signal)
            continue

        if pd.notna(sell_signal) and entry_price is not None:
            exit_price = float(sell_signal)
            trade_return = (exit_price - entry_price) / entry_price
            trades.append(trade_return)
            equity *= 1 + trade_return
            equity_curve.append(equity)
            if trade_return > 0:
                wins += 1
                gross_profit += trade_return
            elif trade_return < 0:
                losses += 1
                gross_loss += abs(trade_return)
            entry_price = None

    trade_count = len(trades)
    win_rate = (wins / trade_count * 100) if trade_count else 0.0
    total_return = (equity - 1.0) * 100
    profit_loss_ratio = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    peak = equity_curve[0]
    max_drawdown = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        drawdown = (peak - value) / peak if peak else 0.0
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    buy_count = int(df["buy_signal"].notna().sum())
    sell_count = int(df["sell_signal"].notna().sum())

    return {
        "win_rate": f"{win_rate:.2f}%",
        "total_return": f"{total_return:.2f}%",
        "profit_loss_ratio": f"{profit_loss_ratio:.2f}",
        "max_drawdown": f"{max_drawdown * 100:.2f}%",
        "trade_count": trade_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
    }


if __name__ == "__main__":
    path = build_chart()
    print(path)
