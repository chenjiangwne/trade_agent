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
) -> dict[str, Any]:
    last_close = float(execution_frame.iloc[-1]["close"])
    last_time = str(pd.to_datetime(execution_frame.iloc[-1]["timestamp"]).isoformat())

    updates: dict[str, Any] = {
        "last_action": decision["action"],
        "last_score": decision["score"],
        "current_phase": "idle",
    }
    state_updates = decision.get("status_updates", {})

    if decision["action"] == "SHORT":
        if status.get("position_status") == "short":
            updates.update(
                {
                    "position_status": "short",
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
                    "position_status": "short",
                    "entry_price": last_close,
                    "entry_time": last_time,
                    "last_entry_score": float(decision.get("entry_score", decision["score"])),
                    "current_phase": "entry_check",
                    "initial_entry_price": float(state_updates.get("initial_entry_price", last_close)),
                    "initial_stop_price": float(state_updates.get("initial_stop_price", 0.0)),
                    "peak_rr": float(state_updates.get("peak_rr", 0.0)),
                }
            )
    elif decision["action"] == "ADD_SHORT":
        updates.update(
            {
                "position_status": "short",
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
        live_order = _place_live_order(config, status, decision)

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


def _place_live_order(config: dict[str, Any], status: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any] | None:
    if decision["action"] not in {"SHORT", "ADD_SHORT", "EXIT"}:
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
        side = "sell"
        params: dict[str, Any] = {}
    else:
        side = "buy"
        params = {"reduceOnly": True}

    logger.warning("placing Binance futures market order: action={} side={} symbol={} amount={} params={}", action, side, exchange_symbol, amount, params)
    order = exchange.create_order(exchange_symbol, "market", side, amount, None, params)
    result = {
        "id": order.get("id"),
        "symbol": order.get("symbol", exchange_symbol),
        "side": order.get("side", side),
        "type": order.get("type", "market"),
        "amount": order.get("amount", amount),
        "status": order.get("status"),
    }
    logger.info("live order response: {}", result)
    return result


def preview_live_order(config: dict[str, Any], status: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any] | None:
    if decision["action"] not in {"SHORT", "ADD_SHORT", "EXIT"}:
        return None

    action = decision["action"]
    return {
        "symbol": _exchange_symbol(config["basic"]["symbol"]),
        "type": "market",
        "side": "sell" if action in {"SHORT", "ADD_SHORT"} else "buy",
        "amount": float(config["trade"]["quantity"]),
        "params": {} if action in {"SHORT", "ADD_SHORT"} else {"reduceOnly": True},
        "action": action,
        "mode": "dry_run",
    }
