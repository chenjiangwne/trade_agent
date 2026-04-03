from __future__ import annotations

from typing import Any

from loguru import logger


def push_failure_message(config: dict[str, Any], title: str, content: str) -> dict[str, Any]:
    channel = config.get("notify", {}).get("channel", "mock")
    payload = {
        "channel": channel,
        "title": title,
        "content": content,
        "status": "mock_sent",
    }
    logger.error("push_message mock sent: {}", payload)
    return payload
