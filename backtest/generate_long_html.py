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


def _add_trade_interval_shapes(fig, trades_df: pd.DataFrame) -> None:
    if trades_df is None or trades_df.empty:
        return

    frame = trades_df.copy()
    frame["entry_time"] = pd.to_datetime(frame["entry_time"], errors="coerce")
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], errors="coerce")
    frame["profit_abs"] = pd.to_numeric(frame["profit_abs"], errors="coerce")
    frame = frame.dropna(subset=["entry_time", "exit_time", "profit_abs"])
    if frame.empty:
        return

    for _, row in frame.iterrows():
        is_win = float(row["profit_abs"]) > 0
        fill = "rgba(14, 203, 129, 0.10)" if is_win else "rgba(246, 70, 93, 0.12)"
        fig.add_shape(
            type="rect",
            x0=row["entry_time"],
            x1=row["exit_time"],
            y0=0,
            y1=1,
            xref="x",
            yref="paper",
            fillcolor=fill,
            line=dict(width=0),
            layer="below",
        )


def generate_interactive_html_with_dashboard(df, backtest_report, output_name="strategy_backtest_dashboard_long.html"):
    logger.info("---- Start Generating Interactive HTML Report with Dashboard ----")

    df = df.copy()
    if "timestamp" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
        df["timestamp"] = df.index

    df = df.reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    if "volume" not in df.columns:
        df["volume"] = 0

    trades_df = backtest_report.get("trades_df") if isinstance(backtest_report, dict) else None

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.94, 0.06],
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
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
        secondary_y=False,
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
                go.Scattergl(
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
                secondary_y=False,
            )

    _add_trade_interval_shapes(fig, trades_df)

    if trades_df is not None and not trades_df.empty:
        if "equity" not in trades_df.columns:
            trades_df["equity"] = trades_df["profit_abs"].cumsum()

        first_entry = trades_df["entry_time"].iloc[0]
        eq_x = [first_entry] + trades_df["exit_time"].tolist()
        eq_y = [0.0] + trades_df["equity"].tolist()
        eq_y_smooth = pd.Series(eq_y).ewm(span=10, adjust=False).mean()

        fig.add_trace(
            go.Scattergl(
                x=eq_x,
                y=eq_y_smooth,
                mode="lines",
                name="Cumulative PnL (Left Axis)",
                line=dict(color="#fbbf24", width=1.6),
                opacity=0.85,
                hovertemplate="累计盈亏<br>时间: %{x}<br>盈亏(USD): %{y:.2f}<extra></extra>",
            ),
            row=1,
            col=1,
            secondary_y=True,
        )

    buy_series = df["buy_signal"] if "buy_signal" in df.columns else pd.Series(index=df.index, dtype=float)
    buy_signals = df[buy_series.notna()]
    if not buy_signals.empty:
        buy_marker_x = [df.loc[index, "timestamp"] for index in buy_signals.index]
        buy_marker_y = [df.loc[index, "low"] * 0.994 for index in buy_signals.index]
        buy_customdata = buy_signals[["timestamp", "buy_signal"]].to_numpy()
        fig.add_trace(
            go.Scattergl(
                x=buy_marker_x,
                y=buy_marker_y,
                customdata=buy_customdata,
                mode="markers",
                name="Long Entry",
                marker=dict(
                    symbol="triangle-up",
                    size=24,
                    color="#facc15",
                    line=dict(width=3, color="#020617"),
                ),
                text=["做多入场"] * len(buy_signals),
                textposition="bottom center",
                textfont=dict(size=12, color="#facc15"),
                hovertemplate=(
                    "LONG ENTRY<br>"
                    "标记K线: %{x}<br>"
                    "信号K线: %{customdata[0]}<br>"
                    "入场价: %{customdata[1]:.2f}<extra></extra>"
                ),
            ),
            row=1,
            col=1,
            secondary_y=False,
        )

    sell_series = df["sell_signal"] if "sell_signal" in df.columns else pd.Series(index=df.index, dtype=float)
    sell_signals = df[sell_series.notna()].copy()
    if not sell_signals.empty:
        sell_signal_types = sell_signals["sell_signal_type"] if "sell_signal_type" in sell_signals.columns else pd.Series("EXIT_SIGNAL", index=sell_signals.index)
        sell_marker_x = [df.loc[index, "timestamp"] for index in sell_signals.index]
        sell_marker_y = [df.loc[index, "high"] * 1.006 for index in sell_signals.index]
        hard_stop_mask = sell_signal_types.eq("HARD_STOP")
        normal_exit_mask = ~hard_stop_mask

        if normal_exit_mask.any():
            normal_sell_signals = sell_signals.loc[normal_exit_mask]
            normal_customdata = normal_sell_signals[["timestamp", "sell_signal"]].to_numpy()
            fig.add_trace(
                go.Scattergl(
                    x=[x for x, keep in zip(sell_marker_x, normal_exit_mask.tolist()) if keep],
                    y=[y for y, keep in zip(sell_marker_y, normal_exit_mask.tolist()) if keep],
                    customdata=normal_customdata,
                    mode="markers",
                    name="Long Exit",
                    marker=dict(
                        symbol="triangle-down",
                        size=22,
                        color="#38bdf8",
                        line=dict(width=3, color="#020617"),
                    ),
                    text=["做多出场"] * len(normal_sell_signals),
                    textposition="top center",
                    textfont=dict(size=12, color="#38bdf8"),
                    hovertemplate=(
                        "LONG EXIT<br>"
                        "标记K线: %{x}<br>"
                        "信号K线: %{customdata[0]}<br>"
                        "出场价: %{customdata[1]:.2f}<extra></extra>"
                    ),
                ),
                row=1,
                col=1,
                secondary_y=False,
            )

        if hard_stop_mask.any():
            hard_stop_signals = sell_signals.loc[hard_stop_mask]
            hard_stop_customdata = hard_stop_signals[["timestamp", "sell_signal"]].to_numpy()
            fig.add_trace(
                go.Scattergl(
                    x=[x for x, keep in zip(sell_marker_x, hard_stop_mask.tolist()) if keep],
                    y=[y for y, keep in zip(sell_marker_y, hard_stop_mask.tolist()) if keep],
                    customdata=hard_stop_customdata,
                    mode="markers",
                    name="Hard Stop",
                    marker=dict(
                        symbol="triangle-down",
                        size=22,
                        color="#ef4444",
                        line=dict(width=3, color="#020617"),
                    ),
                    text=["硬止损出场"] * len(hard_stop_signals),
                    textposition="top center",
                    textfont=dict(size=12, color="#ef4444"),
                    hovertemplate=(
                        "HARD STOP<br>"
                        "标记K线: %{x}<br>"
                        "信号K线: %{customdata[0]}<br>"
                        "出场价: %{customdata[1]:.2f}<extra></extra>"
                    ),
                ),
                row=1,
                col=1,
                secondary_y=False,
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
        margin=dict(t=28, b=28, l=54, r=54),
        xaxis_rangeslider_visible=False,
        transition=dict(duration=0),
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
        secondary_y=False,
        row=1,
        col=1,
    )
    fig.update_yaxes(
        showgrid=False,
        zeroline=True,
        zerolinecolor="rgba(251, 191, 36, 0.2)",
        tickfont=dict(color="#fbbf24", size=11),
        side="left",
        secondary_y=True,
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

    total_trades = _safe_metric(backtest_report, "总交易次数", default=0)
    win_rate = _safe_metric(backtest_report, "胜率", default="0%")
    net_profit = _safe_metric(backtest_report, "总净利润", default="0")
    avg_profit = _safe_metric(backtest_report, "平均盈亏", default=0)
    rr_ratio = _safe_metric(backtest_report, "盈亏比", default=0)
    max_dd = _safe_metric(backtest_report, "最大回撤(USD)", default="0")

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
            <div class="venue">Fixed 1 BTC Base / Auto PnL</div>
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
          <div class="metric-label">Total PnL (USD)</div>
          <div class="metric-value">{net_profit}</div>
        </div>
        <div class="metric-card accent-blue">
          <div class="metric-label">Avg Trade (USD)</div>
          <div class="metric-value">{avg_profit}</div>
        </div>
        <div class="metric-card accent-violet">
          <div class="metric-label">P/L Ratio</div>
          <div class="metric-value">{rr_ratio}</div>
        </div>
        <div class="metric-card accent-red">
          <div class="metric-label">Max DD (USD)</div>
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


def backtest_long_refined(df_4h, fee_rate=0.0004):
    position_queue = []
    trades = []

    for idx, row in df_4h.iterrows():
        buy_price = row.get("buy_signal")
        sell_price = row.get("sell_signal")

        if pd.notna(buy_price) and buy_price > 0:
            position_queue.append({
                "entry_price": buy_price,
                "entry_time": idx,
                "position_size": 1.0,
            })

        if pd.notna(sell_price) and sell_price > 0:
            while position_queue:
                trade_info = position_queue.pop(0)
                entry_price = trade_info["entry_price"]
                position_size = trade_info["position_size"]

                gross_profit_usd = (sell_price - entry_price) * position_size
                fee_usd = (entry_price + sell_price) * fee_rate * position_size
                net_profit_usd = gross_profit_usd - fee_usd

                trades.append({
                    "entry_time": trade_info["entry_time"],
                    "exit_time": idx,
                    "entry_price": entry_price,
                    "exit_price": sell_price,
                    "position_size": position_size,
                    "profit_abs": net_profit_usd,
                })

    if not trades:
        return {
            "总交易次数": 0,
            "胜率": "0%",
            "总净利润": "0.00",
            "平均盈亏": 0,
            "盈亏比": 0,
            "最大回撤(USD)": "0.00",
        }

    trades_df = pd.DataFrame(trades)
    win_rate = (trades_df["profit_abs"] > 0).mean()
    wins = trades_df[trades_df["profit_abs"] > 0]["profit_abs"]
    losses = trades_df[trades_df["profit_abs"] <= 0]["profit_abs"].abs()
    avg_win = wins.mean() if not wins.empty else 0
    avg_loss = losses.mean() if not losses.empty else 0
    rr = (avg_win / avg_loss) if avg_loss != 0 else 0
    trades_df["equity"] = trades_df["profit_abs"].cumsum()
    total_net_profit = trades_df["profit_abs"].sum()
    peak = trades_df["equity"].cummax()
    drawdown_usd = trades_df["equity"] - peak
    max_dd_usd = drawdown_usd.min()

    return {
        "总交易次数": len(trades_df),
        "胜率": f"{win_rate:.2%}",
        "总净利润": f"{total_net_profit:,.2f}",
        "平均盈亏": round(trades_df["profit_abs"].mean(), 2),
        "盈亏比": round(rr, 2),
        "最大回撤(USD)": f"{max_dd_usd:,.2f}",
        "trades_df": trades_df,
    }


def format_trades_table(trades_df):
    columns = ["序号", "入场时间", "出场时间", "入场价格", "出场价格", "净利润(USD)"]
    rows = []
    for number, row in enumerate(trades_df.itertuples(index=False), start=1):
        rows.append(
            [
                str(number),
                str(row.entry_time),
                str(row.exit_time),
                f"{float(row.entry_price):.2f}",
                f"{float(row.exit_price):.2f}",
                f"{float(row.profit_abs):.2f}",
            ]
        )

    widths = [
        max(display_width(column), *(display_width(row[index]) for row in rows))
        for index, column in enumerate(columns)
    ]
    separator = "-+-".join("-" * width for width in widths)
    lines = [
        " | ".join(pad_display_width(column, widths[index]) for index, column in enumerate(columns)),
        separator,
    ]
    for row in rows:
        lines.append(" | ".join(pad_display_width(value, widths[index]) for index, value in enumerate(row)))
    return "\n".join(lines)


def display_width(value):
    text = str(value)
    return sum(2 if ord(char) > 127 else 1 for char in text)


def pad_display_width(value, width):
    text = str(value)
    return text + (" " * max(width - display_width(text), 0))


if __name__ == "__main__":
    four_path = "backtest_result_long_signals.xlsx"
    if os.path.exists(four_path):
        df_4h = pd.read_excel(four_path)
        df_4h.set_index("timestamp", inplace=True)

        report_stats = backtest_long_refined(df_4h)
        generate_interactive_html_with_dashboard(df_4h, report_stats)

        print("\n" + "=" * 40)
        print("BTC 每次固定 1 BTC 回测报告 (绝对金额)")
        print("=" * 40)
        for key, value in report_stats.items():
            if key == "trades_df":
                print("\n交易明细:")
                print(format_trades_table(value))
            else:
                print(f"{key}: {value}")
    else:
        logger.warning(f"数据文件 {four_path} 不存在。请准备好该文件后运行脚本。")
