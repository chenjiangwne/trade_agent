from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generic.Common import yml_reader


OUTPUT_DIR = PROJECT_ROOT / "backtest"
OUTPUT_XLSX = OUTPUT_DIR / "bull_market_daily_filter.xlsx"
OUTPUT_HTML = OUTPUT_DIR / "bull_market_kline.html"


def load_daily_data() -> pd.DataFrame:
    config = yml_reader(str(PROJECT_ROOT / "config" / "config.yaml"))
    daily_path = PROJECT_ROOT / config["data"]["kline_1d_file"]
    df = pd.read_excel(daily_path)
    df.columns = [str(column).strip().lower() for column in df.columns]
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df[["timestamp", "open", "high", "low", "close", "volume"]].copy()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame["ema50"] = frame["close"].ewm(span=50, adjust=False).mean()
    frame["ema200"] = frame["close"].ewm(span=200, adjust=False).mean()
    frame["ema50_slope_5"] = frame["ema50"] - frame["ema50"].shift(5)
    frame["ema200_slope_20"] = frame["ema200"] - frame["ema200"].shift(20)
    frame["rolling_high_60"] = frame["close"].rolling(60).max()
    frame["rolling_low_60"] = frame["close"].rolling(60).min()
    frame["drawdown_from_high_60"] = frame["close"] / frame["rolling_high_60"] - 1
    frame["range_position_60"] = (frame["close"] - frame["rolling_low_60"]) / (
        (frame["rolling_high_60"] - frame["rolling_low_60"]) + 1e-9
    )

    tr = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - frame["close"].shift()).abs(),
            (frame["low"] - frame["close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame["atr14"] = tr.rolling(14).mean()
    frame["atr_pct"] = frame["atr14"] / frame["close"]

    plus_dm = frame["high"].diff().clip(lower=0)
    minus_dm = (-frame["low"].diff()).clip(lower=0)
    tr_smooth = tr.ewm(alpha=1 / 14).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / 14).mean() / (tr_smooth + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / 14).mean() / (tr_smooth + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    frame["adx14"] = dx.ewm(alpha=1 / 14).mean()
    return frame


def apply_bull_filter(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame["price_above_ema200"] = frame["close"] > frame["ema200"]
    frame["ema50_above_ema200"] = frame["ema50"] > frame["ema200"]
    frame["ema50_rising"] = frame["ema50_slope_5"] > 0
    frame["ema200_rising"] = frame["ema200_slope_20"] > 0
    frame["adx_strong"] = frame["adx14"] >= 18
    frame["close_not_far_below_ema50"] = frame["close"] >= frame["ema50"] * 0.98
    frame["near_high_60"] = frame["range_position_60"] >= 0.70
    frame["drawdown_ok_60"] = frame["drawdown_from_high_60"] >= -0.18

    frame["price_above_ema200_days"] = (
        frame["price_above_ema200"].astype(int).groupby((~frame["price_above_ema200"]).cumsum()).cumsum()
    )
    frame["ema50_above_ema200_days"] = (
        frame["ema50_above_ema200"].astype(int).groupby((~frame["ema50_above_ema200"]).cumsum()).cumsum()
    )
    frame["long_bull_persistence"] = (
        (frame["price_above_ema200_days"] >= 40) & (frame["ema50_above_ema200_days"] >= 40)
    )

    frame["bull_trend_filter"] = (
        frame["long_bull_persistence"]
        & frame["ema50_rising"]
        & frame["ema200_rising"]
        & frame["adx_strong"]
        & frame["close_not_far_below_ema50"]
        & frame["near_high_60"]
        & frame["drawdown_ok_60"]
    )

    frame["bull_score"] = (
        frame["long_bull_persistence"].astype(int) * 3
        + frame["ema50_rising"].astype(int)
        + frame["ema200_rising"].astype(int)
        + frame["adx_strong"].astype(int)
        + frame["close_not_far_below_ema50"].astype(int)
        + frame["near_high_60"].astype(int)
        + frame["drawdown_ok_60"].astype(int)
    )
    return frame


def build_bull_segments(df: pd.DataFrame) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    start = None
    for _, row in df.iterrows():
        current_time = row["timestamp"].strftime("%Y-%m-%d")
        if bool(row["bull_trend_filter"]) and start is None:
            start = current_time
        elif not bool(row["bull_trend_filter"]) and start is not None:
            segments.append((start, current_time))
            start = None
    if start is not None:
        segments.append((start, df.iloc[-1]["timestamp"].strftime("%Y-%m-%d")))
    return segments


def build_summary(df: pd.DataFrame) -> dict[str, str]:
    filtered = df[df["bull_trend_filter"]].copy()
    total_days = len(df)
    bull_days = len(filtered)
    bull_ratio = (bull_days / total_days * 100) if total_days else 0.0
    median_distance = ((filtered["close"] / filtered["ema200"]) - 1).median() * 100 if bull_days else 0.0
    median_adx = filtered["adx14"].median() if bull_days else 0.0
    median_atr_pct = filtered["atr_pct"].median() * 100 if bull_days else 0.0
    return {
        "bull_days": f"{bull_days}",
        "bull_ratio": f"{bull_ratio:.2f}%",
        "median_distance": f"{median_distance:.2f}%",
        "median_adx": f"{median_adx:.2f}",
        "median_atr_pct": f"{median_atr_pct:.2f}%",
    }


def render_html(df: pd.DataFrame) -> Path:
    categories = df["timestamp"].dt.strftime("%Y-%m-%d").tolist()
    kline_data = df[["open", "close", "low", "high"]].round(6).values.tolist()
    ema50 = df["ema50"].round(6).bfill().ffill().tolist()
    ema200 = df["ema200"].round(6).bfill().ffill().tolist()
    bull_segments = build_bull_segments(df)
    summary = build_summary(df)

    mark_areas = [
        [{"xAxis": start, "itemStyle": {"color": "rgba(25, 195, 125, 0.12)"}}, {"xAxis": end}]
        for start, end in bull_segments
    ]

    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Bull Market Filter</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root {{
      --bg: #09111f;
      --panel: #111b33;
      --text: #d9e3ff;
      --muted: #93a8db;
      --card: rgba(255,255,255,0.04);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: radial-gradient(circle at top, #16254f 0%, var(--bg) 55%); color: var(--text); font-family: Consolas, "Microsoft YaHei", monospace; }}
    .wrap {{ width: min(96vw, 1800px); margin: 0 auto; padding: 20px 16px 28px; }}
    .title {{ font-size: 24px; font-weight: 700; }}
    .meta {{ color: var(--muted); margin-top: 6px; font-size: 13px; }}
    .summary {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; margin: 18px 0; }}
    .card {{ background: var(--card); border: 1px solid rgba(143, 161, 212, 0.18); border-radius: 14px; padding: 14px 16px; }}
    .label {{ color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
    .value {{ font-size: 24px; font-weight: 700; }}
    .panel {{ background: rgba(17, 27, 51, 0.88); border: 1px solid rgba(143, 161, 212, 0.18); border-radius: 18px; overflow: hidden; }}
    #chart {{ width: 100%; height: 880px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="title">BTC 日线长牛趋势过滤</div>
    <div class="meta">规则: EMA50 > EMA200 持续至少 40 天, close > EMA200 持续至少 40 天, EMA200 上升, EMA50 上升, ADX >= 18, 近 60 天靠近高位, 近 60 天回撤不深</div>
    <div class="summary">
      <div class="card"><div class="label">长牛过滤天数</div><div class="value">{summary["bull_days"]}</div></div>
      <div class="card"><div class="label">长牛占比</div><div class="value">{summary["bull_ratio"]}</div></div>
      <div class="card"><div class="label">价格高于 EMA200 中位值</div><div class="value">{summary["median_distance"]}</div></div>
      <div class="card"><div class="label">ADX 中位值</div><div class="value">{summary["median_adx"]}</div></div>
      <div class="card"><div class="label">ATR 占比中位值</div><div class="value">{summary["median_atr_pct"]}</div></div>
    </div>
    <div class="panel"><div id="chart"></div></div>
  </div>
  <script>
    const categories = {json.dumps(categories, ensure_ascii=False)};
    const klineData = {json.dumps(kline_data, ensure_ascii=False)};
    const ema50 = {json.dumps(ema50, ensure_ascii=False)};
    const ema200 = {json.dumps(ema200, ensure_ascii=False)};
    const markAreas = {json.dumps(mark_areas, ensure_ascii=False)};
    const chart = echarts.init(document.getElementById('chart'));
    chart.setOption({{
      backgroundColor: 'transparent',
      animation: false,
      legend: {{ top: 12, textStyle: {{ color: '#d9e3ff' }} }},
      tooltip: {{
        trigger: 'axis',
        axisPointer: {{ type: 'cross' }},
        backgroundColor: 'rgba(12,18,38,0.95)',
        borderColor: '#31457f',
        textStyle: {{ color: '#e7ecff' }}
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
          itemStyle: {{
            color: '#18b26b',
            color0: '#e04f5f',
            borderColor: '#18b26b',
            borderColor0: '#e04f5f'
          }},
          markArea: {{ silent: true, data: markAreas }}
        }},
        {{
          name: 'EMA50',
          type: 'line',
          data: ema50,
          showSymbol: false,
          smooth: true,
          lineStyle: {{ width: 1.5, color: '#ffd166' }}
        }},
        {{
          name: 'EMA200',
          type: 'line',
          data: ema200,
          showSymbol: false,
          smooth: true,
          lineStyle: {{ width: 1.5, color: '#5da9ff' }}
        }}
      ]
    }});
    window.addEventListener('resize', () => chart.resize());
  </script>
</body>
</html>
'''
    OUTPUT_HTML.write_text(html, encoding='utf-8')
    return OUTPUT_HTML


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df = load_daily_data()
    df = add_indicators(df)
    df = apply_bull_filter(df)
    df.to_excel(OUTPUT_XLSX, index=False)
    render_html(df)
    print(OUTPUT_XLSX)
    print(OUTPUT_HTML)


if __name__ == '__main__':
    main()
