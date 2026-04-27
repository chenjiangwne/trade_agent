from __future__ import annotations

import os
from typing import Any

import ccxt
import pandas as pd
from loguru import logger

from generic.network import ccxt_proxy_config, resolve_proxy_url


def execute_order(
    config: dict[str, Any],
    status: dict[str, Any],
    decision: dict[str, Any],
    execution_frame: pd.DataFrame,
) -> dict[str, Any]:
    last_close = float(execution_frame.iloc[-1]["close"])
    last_time = str(pd.to_datetime(execution_frame.iloc[-1]["timestamp"]).isoformat())

    updates: dict[str, Any] = {
        "last_action": decision["action"],
        "last_score": decision["score"],
        "current_phase": "idle",
    }

    if decision["action"] in {"SHORT", "LONG"}:
        position_status = "short" if decision["action"] == "SHORT" else "long"
        updates.update(
            {
                "position_status": position_status,
                "entry_price": last_close,
                "entry_time": last_time,
                "current_phase": "entry_check",
            }
        )
    elif decision["action"] == "EXIT":
        updates.update(
            {
                "position_status": "flat",
                "entry_price": 0.0,
                "entry_time": "",
                "current_phase": "exit_check",
            }
        )
    else:
        updates["position_status"] = status["position_status"]
        updates["entry_price"] = status["entry_price"]
        updates["entry_time"] = status["entry_time"]

    if not config["trade"]["paper_trade"]:
        _log_live_execution_request(config, status, decision)

    return {
        "mode": "paper" if config["trade"]["paper_trade"] else "live",
        "status_updates": updates,
    }


def _log_live_execution_request(config: dict[str, Any], status: dict[str, Any], decision: dict[str, Any]) -> None:
    if decision["action"] not in {"LONG", "SHORT", "EXIT"}:
        return

    api_key = os.getenv("BINANCE_API_KEY", "")
    secret = os.getenv("BINANCE_SECRET_KEY", "")
    if not api_key or not secret:
        logger.warning("live trading requested but BINANCE_API_KEY / BINANCE_SECRET_KEY are not set; skip order placement")
        return

    symbol = config["basic"]["symbol"]
    exchange_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
    if decision["action"] == "SHORT":
        logger.warning("live short entry requested, but exchange client is configured for spot; skip live order placement")
        return
    if decision["action"] == "LONG":
        side = "buy"
    else:
        side = "buy" if status.get("position_status") == "short" else "sell"
    amount = float(config["trade"]["quantity"])
    proxy = resolve_proxy_url(config)

    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
            "proxies": ccxt_proxy_config(config),
            "options": {"adjustForTimeDifference": True},
        }
    )
    logger.warning(
        "placing live {} market order: {} {} proxy={}",
        side,
        exchange_symbol,
        amount,
        proxy or "DIRECT",
    )
    order = exchange.create_order(exchange_symbol, "market", side, amount)
    logger.info("live order response: {}", order.get("id", "unknown"))
