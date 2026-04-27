from __future__ import annotations

import os
from typing import Any


DIRECT_PROXY_MARKERS = {"", "direct", "off", "none", "false", "0"}


def resolve_proxy_url(config: dict[str, Any]) -> str | None:
    env_proxy = os.getenv("TRADE_AGENT_PROXY")
    if env_proxy is not None:
        normalized = env_proxy.strip()
        if normalized.lower() in DIRECT_PROXY_MARKERS:
            return None
        return normalized

    config_proxy = str(config.get("network", {}).get("proxy", "") or "").strip()
    if config_proxy.lower() in DIRECT_PROXY_MARKERS:
        return None
    return config_proxy


def ccxt_proxy_config(config: dict[str, Any]) -> dict[str, Any]:
    proxy = resolve_proxy_url(config)
    if not proxy:
        return {}
    return {"http": proxy, "https": proxy}
