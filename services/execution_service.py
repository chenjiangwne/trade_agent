from __future__ import annotations

import os
from typing import Any

import ccxt
import pandas as pd
from loguru import logger


def execute_order(
    config: dict[str, Any],
    status: dict[str, Any],
    decision: dict[str, Any],
    execution_frame: pd.DataFrame,
    side: str = "short",
) -> dict[str, Any]:
    last_close = float(execution_frame.iloc[-1]["close"])
    last_time = str(pd.to_datetime(execution_frame.iloc[-1]["timestamp"]).isoformat())
    entry_action = "SHORT" if side == "short" else "LONG"
    add_action = "ADD_SHORT" if side == "short" else "ADD_LONG"

    updates: dict[str, Any] = {
        "last_action": decision["action"],
        "last_score": decision["score"],
        "current_phase": "idle",
    }
    state_updates = decision.get("status_updates", {})

    if decision["action"] == entry_action:
        if status.get("position_status") == side:
            updates.update(
                {
                    "position_status": side,
                    "entry_price": status["entry_price"],
                    "entry_time": status["entry_time"],
                    "last_entry_score": float(decision.get("entry_score", decision["score"])),
                    "current_phase": "entry_check",
                    "initial_entry_price": float(state_updates.get("initial_entry_price", status.get("initial_entry_price", status["entry_price"]))),
                    "initial_stop_price": float(state_updates.get("initial_stop_price", status.get("initial_stop_price", 0.0))),
                    "peak_rr": float(state_updates.get("peak_rr", status.get("peak_rr", 0.0))),
                }
            )
        else:
            updates.update(
                {
                    "position_status": side,
                    "entry_price": last_close,
                    "entry_time": last_time,
                    "last_entry_score": float(decision.get("entry_score", decision["score"])),
                    "current_phase": "entry_check",
                    "initial_entry_price": float(state_updates.get("initial_entry_price", last_close)),
                    "initial_stop_price": float(state_updates.get("initial_stop_price", 0.0)),
                    "peak_rr": float(state_updates.get("peak_rr", 0.0)),
                }
            )
    elif decision["action"] == add_action:
        updates.update(
            {
                "position_status": side,
                "entry_price": status["entry_price"],
                "entry_time": status["entry_time"],
                "last_entry_score": float(decision.get("entry_score", decision["score"])),
                "current_phase": "entry_check",
                "initial_entry_price": float(state_updates.get("initial_entry_price", status.get("initial_entry_price", status["entry_price"]))),
                "initial_stop_price": float(state_updates.get("initial_stop_price", status.get("initial_stop_price", 0.0))),
                "peak_rr": float(state_updates.get("peak_rr", status.get("peak_rr", 0.0))),
            }
        )
    elif decision["action"] == "EXIT":
        updates.update(
            {
                "position_status": "flat",
                "entry_price": 0.0,
                "entry_time": "",
                "last_entry_score": 0.0,
                "current_phase": "exit_check",
                "initial_entry_price": 0.0,
                "initial_stop_price": 0.0,
                "peak_rr": 0.0,
            }
        )
    else:
        updates["position_status"] = status["position_status"]
        updates["entry_price"] = status["entry_price"]
        updates["entry_time"] = status["entry_time"]
        updates["last_entry_score"] = float(status.get("last_entry_score", 0.0))
        updates["initial_entry_price"] = float(state_updates.get("initial_entry_price", status.get("initial_entry_price", status["entry_price"])))
        updates["initial_stop_price"] = float(state_updates.get("initial_stop_price", status.get("initial_stop_price", 0.0)))
        updates["peak_rr"] = float(state_updates.get("peak_rr", status.get("peak_rr", 0.0)))

    live_order = None
    if not config["trade"]["paper_trade"]:
        live_order = _place_live_order(config, status, decision, side=side)

    return {
        "mode": "paper" if config["trade"]["paper_trade"] else "live",
        "status_updates": updates,
        "live_order": live_order,
    }


def _exchange_symbol(symbol: str) -> str:
    if ":" in symbol and "/" in symbol:
        return symbol
    if "/" in symbol:
        base, quote = symbol.split("/", 1)
        return f"{base}/{quote}:{quote}"
    quote = symbol[-4:]
    base = symbol[:-4]
    return f"{base}/{quote}:{quote}"


def _build_live_exchange(config: dict[str, Any]) -> ccxt.Exchange:
    api_key = os.getenv("BINANCE_API_KEY", "")
    secret = os.getenv("BINANCE_SECRET_KEY", "")
    if not api_key or not secret:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_SECRET_KEY are not set")

    proxy = config.get("network", {}).get("proxy")
    exchange_config: dict[str, Any] = {
        "apiKey": api_key,
        "secret": secret,
        "enableRateLimit": True,
        "options": {
            "adjustForTimeDifference": True,
            "defaultType": "future",
        },
    }
    if proxy:
        exchange_config["proxies"] = {"http": proxy, "https": proxy}
    return ccxt.binance(exchange_config)


def _place_live_order(config: dict[str, Any], status: dict[str, Any], decision: dict[str, Any], side: str = "short") -> dict[str, Any] | None:
    if decision["action"] not in {"SHORT", "ADD_SHORT", "LONG", "ADD_LONG", "EXIT"}:
        return None

    try:
        exchange = _build_live_exchange(config)
    except RuntimeError as exc:
        logger.warning("live trading requested but {}; skip order placement", exc)
        return None

    exchange_symbol = _exchange_symbol(config["basic"]["symbol"])
    amount = float(config["trade"]["quantity"])
    action = decision["action"]

    if action in {"SHORT", "ADD_SHORT"}:
        order_side = "sell"
        params: dict[str, Any] = {}
    elif action in {"LONG", "ADD_LONG"}:
        order_side = "buy"
        params = {}
    elif side == "long":
        order_side = "sell"
        params = {"reduceOnly": True}
    else:
        order_side = "buy"
        params = {"reduceOnly": True}

    logger.warning("placing Binance futures market order: action={} side={} symbol={} amount={} params={}", action, order_side, exchange_symbol, amount, params)
    order = exchange.create_order(exchange_symbol, "market", order_side, amount, None, params)
    result = {
        "id": order.get("id"),
        "symbol": order.get("symbol", exchange_symbol),
        "side": order.get("side", order_side),
        "type": order.get("type", "market"),
        "amount": order.get("amount", amount),
        "status": order.get("status"),
    }
    logger.info("live order response: {}", result)
    return result


def preview_live_order(config: dict[str, Any], status: dict[str, Any], decision: dict[str, Any], side: str = "short") -> dict[str, Any] | None:
    if decision["action"] not in {"SHORT", "ADD_SHORT", "LONG", "ADD_LONG", "EXIT"}:
        return None

    action = decision["action"]
    if action in {"SHORT", "ADD_SHORT"}:
        order_side = "sell"
        params: dict[str, Any] = {}
    elif action in {"LONG", "ADD_LONG"}:
        order_side = "buy"
        params = {}
    elif side == "long":
        order_side = "sell"
        params = {"reduceOnly": True}
    else:
        order_side = "buy"
        params = {"reduceOnly": True}

    return {
        "symbol": _exchange_symbol(config["basic"]["symbol"]),
        "type": "market",
        "side": order_side,
        "amount": float(config["trade"]["quantity"]),
        "params": params,
        "action": action,
        "mode": "dry_run",
    }
