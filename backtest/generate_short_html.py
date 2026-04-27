import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from loguru import logger
from plotly.subplots import make_subplots


def _pick_series_color(name: str) -> str:
    palette = {
        "bb_mid": "#f59e0b",
        "ema200": "#3b82f6",
        "ema120": "#8b5cf6",
        "ema60": "#22c55e",
        "ema20": "#f97316",
    }
    return palette.get(name, "#94a3b8")


def _safe_metric(report: dict, *keys, default=0):
    for key in keys:
        if key in report:
            return report[key]
    return default


def generate_interactive_html_with_dashboard(df, backtest_report, output_name="strategy_backtest_dashboard.html"):
    """
    df: 包含 K 线、买卖信号、EMA/BB 等指标的 DataFrame
    backtest_report: BacktestEngine().run_from_df() 返回的统计字典
    """
    logger.info("---- Start Generating Interactive HTML Report with Dashboard ----")

    df = df.copy()
    if "timestamp" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
        df["timestamp"] = df.index

    df = df.reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    if "volume" not in df.columns:
        df["volume"] = 0

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.78, 0.22],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]],
    )

    increasing_color = "#0ecb81"
    decreasing_color = "#f6465d"
    grid_color = "rgba(148, 163, 184, 0.10)"
    text_color = "#cbd5e1"
    paper_bg = "#0b1220"
    plot_bg = "#111827"

    fig.add_trace(
        go.Candlestick(
            x=df["timestamp"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="BTC/USDT",
            increasing=dict(line=dict(color=increasing_color, width=1), fillcolor=increasing_color),
            decreasing=dict(line=dict(color=decreasing_color, width=1), fillcolor=decreasing_color),
        ),
        row=1,
        col=1,
    )

    overlay_candidates = [
        ("bb_mid", "Bollinger Mid"),
        ("ema200", "EMA200"),
        ("ema120", "EMA120"),
        ("ema60", "EMA60"),
        ("ema20", "EMA20"),
    ]
    for col, label in overlay_candidates:
        if col in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["timestamp"],
                    y=df[col],
                    mode="lines",
                    name=label,
                    line=dict(width=1.4, color=_pick_series_color(col)),
                    opacity=0.95,
                    hovertemplate=f"{label}: %{{y:.2f}}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    buy_series = df["buy_signal"] if "buy_signal" in df.columns else pd.Series(index=df.index, dtype=float)
    buy_signals = df[buy_series.notna()]
    if not buy_signals.empty:
        fig.add_trace(
            go.Scatter(
                x=buy_signals["timestamp"],
                y=buy_signals["high"] * 1.008,
                mode="markers",
                name="Short Entry",
                marker=dict(
                    symbol="triangle-down",
                    size=16,
                    color="#f6465d",
                    line=dict(width=2, color="#f8fafc"),
                ),
                hovertemplate="SHORT ENTRY<br>%{x}<br>%{y:.2f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    sell_series = df["sell_signal"] if "sell_signal" in df.columns else pd.Series(index=df.index, dtype=float)
    sell_signals = df[sell_series.notna()]
    if not sell_signals.empty:
        fig.add_trace(
            go.Scatter(
                x=sell_signals["timestamp"],
                y=sell_signals["low"] * 0.992,
                mode="markers",
                name="Short Exit",
                marker=dict(
                    symbol="triangle-up",
                    size=16,
                    color="#22c55e",
                    line=dict(width=2, color="#f8fafc"),
                ),
                hovertemplate="SHORT EXIT<br>%{x}<br>%{y:.2f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    volume_colors = np.where(df["close"] >= df["open"], increasing_color, decreasing_color)
    fig.add_trace(
        go.Bar(
            x=df["timestamp"],
            y=df["volume"],
            name="Volume",
            marker=dict(color=volume_colors, opacity=0.85),
            hovertemplate="Volume<br>%{x}<br>%{y:,.0f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        height=920,
        hovermode="x unified",
        dragmode="pan",
        margin=dict(t=28, b=28, l=54, r=28),
        xaxis_rangeslider_visible=False,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(15, 23, 42, 0.55)",
            bordercolor="rgba(148, 163, 184, 0.18)",
            borderwidth=1,
            font=dict(color=text_color, size=11),
        ),
        hoverlabel=dict(
            bgcolor="#020617",
            bordercolor="#334155",
            font=dict(color="#e2e8f0", size=12),
        ),
    )

    fig.update_xaxes(
        showgrid=True,
        gridcolor=grid_color,
        showspikes=True,
        spikemode="across",
        spikecolor="rgba(148,163,184,0.35)",
        spikethickness=1,
        linecolor="rgba(148, 163, 184, 0.28)",
        tickfont=dict(color=text_color, size=11),
        rangeslider_visible=False,
        row=1,
        col=1,
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=grid_color,
        showspikes=True,
        spikemode="across",
        spikecolor="rgba(148,163,184,0.35)",
        spikethickness=1,
        linecolor="rgba(148, 163, 184, 0.28)",
        tickfont=dict(color=text_color, size=11),
        row=2,
        col=1,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=grid_color,
        tickfont=dict(color=text_color, size=11),
        side="right",
        row=1,
        col=1,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=grid_color,
        tickfont=dict(color=text_color, size=11),
        side="right",
        row=2,
        col=1,
    )

    total_trades = _safe_metric(backtest_report, "总交易次数", "鎬讳氦鏄撴鏁?", default=0)
    win_rate = _safe_metric(backtest_report, "胜率", "鑳滅巼", default="0%")
    net_profit = _safe_metric(backtest_report, "累计净收益率", "绱鍑€鏀剁泭鐜?", default="0%")
    profit_abs = _safe_metric(backtest_report, "总绝对利润", "鎬荤粷瀵瑰埄娑?", default=0)
    rr_ratio = _safe_metric(backtest_report, "盈亏比", "鐩堜簭姣?", default=0)
    max_dd = _safe_metric(backtest_report, "最大回撤", "鏈€澶у洖鎾?", default="0%")

    last_close = float(df["close"].iloc[-1]) if not df.empty else 0.0
    price_change = float(df["close"].iloc[-1] - df["close"].iloc[-2]) if len(df) > 1 else 0.0
    price_change_pct = (price_change / float(df["close"].iloc[-2])) if len(df) > 1 and float(df["close"].iloc[-2]) != 0 else 0.0
    price_color = increasing_color if price_change >= 0 else decreasing_color

    dashboard_html = f"""
    <div class="terminal-shell">
      <div class="hero-bar">
        <div>
          <div class="eyebrow">Strategy Dashboard</div>
          <div class="symbol-row">
            <div class="symbol">BTC/USDT</div>
            <div class="venue">Perp-style Backtest Panel</div>
          </div>
        </div>
        <div class="live-price-block">
          <div class="live-label">Last Close</div>
          <div class="live-price" style="color: {price_color};">{last_close:,.2f}</div>
          <div class="live-change" style="color: {price_color};">{price_change:+,.2f} ({price_change_pct:+.2%})</div>
        </div>
      </div>

      <div class="metrics-grid">
        <div class="metric-card accent-green">
          <div class="metric-label">Trades</div>
          <div class="metric-value">{total_trades}</div>
        </div>
        <div class="metric-card accent-green-soft">
          <div class="metric-label">Win Rate</div>
          <div class="metric-value">{win_rate}</div>
        </div>
        <div class="metric-card accent-amber">
          <div class="metric-label">Net Return</div>
          <div class="metric-value">{net_profit}</div>
        </div>
        <div class="metric-card accent-blue">
          <div class="metric-label">Profit USD</div>
          <div class="metric-value">{profit_abs}</div>
        </div>
        <div class="metric-card accent-violet">
          <div class="metric-label">P/L Ratio</div>
          <div class="metric-value">{rr_ratio}</div>
        </div>
        <div class="metric-card accent-red">
          <div class="metric-label">Max Drawdown</div>
          <div class="metric-value">{max_dd}</div>
        </div>
      </div>
    </div>
    """

    final_html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>BTC Strategy Dashboard</title>
      <style>
        :root {{
          --bg: #070b14;
          --panel: #0f172a;
          --panel-2: #111827;
          --text: #e5e7eb;
          --muted: #94a3b8;
          --border: rgba(148, 163, 184, 0.16);
          --green: #0ecb81;
          --red: #f6465d;
          --amber: #f59e0b;
          --blue: #38bdf8;
          --violet: #8b5cf6;
        }}
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          color: var(--text);
          background:
            radial-gradient(circle at top left, rgba(14, 203, 129, 0.08), transparent 24%),
            radial-gradient(circle at top right, rgba(56, 189, 248, 0.10), transparent 22%),
            linear-gradient(180deg, #030712 0%, #0b1220 100%);
          font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
        }}
        .page {{
          width: min(99vw, 1920px);
          margin: 0 auto;
          padding: 10px 10px 20px;
        }}
        .terminal-shell {{
          background: linear-gradient(180deg, rgba(15, 23, 42, 0.96), rgba(2, 6, 23, 0.92));
          border: 1px solid var(--border);
          border-radius: 18px;
          padding: 18px;
          box-shadow: 0 24px 80px rgba(2, 6, 23, 0.45);
          margin-bottom: 16px;
        }}
        .hero-bar {{
          display: flex;
          justify-content: space-between;
          align-items: flex-end;
          gap: 16px;
          margin-bottom: 16px;
          flex-wrap: wrap;
        }}
        .eyebrow {{
          color: var(--muted);
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.12em;
          margin-bottom: 6px;
        }}
        .symbol-row {{ display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }}
        .symbol {{ font-size: 34px; font-weight: 700; letter-spacing: 0.02em; }}
        .venue {{ color: var(--muted); font-size: 14px; }}
        .live-price-block {{ text-align: right; }}
        .live-label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }}
        .live-price {{ font-size: 30px; font-weight: 700; line-height: 1.1; margin-top: 4px; }}
        .live-change {{ font-size: 14px; margin-top: 4px; }}
        .metrics-grid {{
          display: grid;
          grid-template-columns: repeat(6, minmax(0, 1fr));
          gap: 12px;
        }}
        .metric-card {{
          position: relative;
          overflow: hidden;
          background: linear-gradient(180deg, rgba(17, 24, 39, 0.96), rgba(15, 23, 42, 0.82));
          border: 1px solid var(--border);
          border-radius: 16px;
          padding: 14px 14px 16px;
          min-height: 92px;
        }}
        .metric-card::before {{
          content: "";
          position: absolute;
          inset: 0 auto 0 0;
          width: 4px;
          background: currentColor;
          opacity: 0.95;
        }}
        .metric-label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }}
        .metric-value {{ font-size: 26px; font-weight: 700; margin-top: 12px; }}
        .accent-green {{ color: var(--green); }}
        .accent-green-soft {{ color: #22c55e; }}
        .accent-amber {{ color: var(--amber); }}
        .accent-blue {{ color: var(--blue); }}
        .accent-violet {{ color: var(--violet); }}
        .accent-red {{ color: var(--red); }}
        .chart-shell {{
          background: linear-gradient(180deg, rgba(15, 23, 42, 0.95), rgba(2, 6, 23, 0.92));
          border: 1px solid var(--border);
          border-radius: 18px;
          padding: 6px;
          box-shadow: 0 20px 70px rgba(2, 6, 23, 0.35);
        }}
        .js-plotly-plot .plotly .modebar {{ left: 12px; top: 12px; }}
        @media (max-width: 1280px) {{
          .metrics-grid {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
        }}
        @media (max-width: 760px) {{
          .page {{ width: 100vw; padding: 8px 8px 16px; }}
          .symbol {{ font-size: 26px; }}
          .live-price {{ font-size: 24px; }}
          .metrics-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
          .metric-value {{ font-size: 22px; }}
        }}
      </style>
    </head>
    <body>
      <div class="page">
        {dashboard_html}
        <div class="chart-shell">{fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displaylogo': False, 'responsive': True, 'scrollZoom': True})}</div>
      </div>
    </body>
    </html>
    """

    with open(output_name, "w", encoding="utf-8") as f:
        f.write(final_html_content)

    logger.success(f"Interactive dashboard generated: {os.path.abspath(output_name)}")


def backtest_short_refined(df_4h, fee_rate=0.0004):
    """
    完善后的指标计算方法
    fee_rate: 单边手续费，默认万四
    """
    position_queue = []
    trades = []

    for idx, row in df_4h.iterrows():
        buy_price = row.get("buy_signal")
        sell_price = row.get("sell_signal")

        if pd.notna(buy_price) and buy_price > 0:
            position_queue.append({
                "entry_price": buy_price,
                "entry_time": idx,
            })

        if pd.notna(sell_price) and sell_price > 0:
            while position_queue:
                trade_info = position_queue.pop(0)
                entry_price = trade_info["entry_price"]
                gross_return = (entry_price - sell_price) / entry_price
                net_return = gross_return - (fee_rate * 2)
                profit_abs = entry_price - sell_price

                trades.append({
                    "entry_time": trade_info["entry_time"],
                    "exit_time": idx,
                    "entry_price": entry_price,
                    "exit_price": sell_price,
                    "net_return": net_return,
                    "profit_abs": profit_abs,
                })

    if not trades:
        return {
            "总交易次数": 0,
            "胜率": "0%",
            "累计净收益率": "0%",
            "总绝对利润": 0,
            "盈亏比": 0,
            "最大回撤": "0%",
        }

    trades_df = pd.DataFrame(trades)
    win_rate = (trades_df["net_return"] > 0).mean()
    cum_return = (1 + trades_df["net_return"]).prod() - 1
    wins = trades_df[trades_df["net_return"] > 0]["net_return"]
    losses = trades_df[trades_df["net_return"] <= 0]["net_return"].abs()
    rr = (wins.mean() / losses.mean()) if not wins.empty and not losses.empty and losses.mean() != 0 else 0
    equity_curve = pd.concat([pd.Series([1.0]), (1 + trades_df["net_return"]).cumprod()], ignore_index=True)
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    max_dd = drawdown.min()

    return {
        "总交易次数": len(trades_df),
        "胜率": f"{win_rate:.2%}",
        "累计净收益率": f"{'+' if cum_return > 0 else ''}{cum_return:.2%}",
        "总绝对利润": round(trades_df["profit_abs"].sum(), 2),
        "盈亏比": round(rr, 2),
        "最大回撤": f"{max_dd:.2%}",
        "trades_df": trades_df,
    }


if __name__ == "__main__":
    four_path = "backtest_result_with_signals.xlsx"
    df_4h = pd.read_excel(four_path)
    df_4h.set_index("timestamp", inplace=True)

    report_stats = backtest_short_refined(df_4h)
    generate_interactive_html_with_dashboard(df_4h, report_stats)

    print("\n" + "=" * 30)
    print("BTC 近3年自动化回测报告")
    print("=" * 30)
    for key, value in report_stats.items():
        print(f"{key}: {value}")
