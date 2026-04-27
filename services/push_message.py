from __future__ import annotations

import json
import urllib.request
from typing import Any

from loguru import logger


WECOM_MARKDOWN_LIMIT = 4000


def push_message(config: dict[str, Any], title: str, content: str) -> dict[str, Any]:
    channel = config.get("notify", {}).get("channel", "mock")
    if channel == "wecom":
        return _push_wecom_message(config, title, content)

    payload = {
        "channel": channel,
        "title": title,
        "content": content,
        "status": "mock_sent",
    }
    logger.info("push_message mock sent: {}", payload)
    return payload


def push_failure_message(config: dict[str, Any], title: str, content: str) -> dict[str, Any]:
    payload = push_message(config, title, content)
    logger.error("push_failure_message sent: {}", payload)
    return payload


def _push_wecom_message(config: dict[str, Any], title: str, content: str) -> dict[str, Any]:
    webhook = str(config.get("notify", {}).get("webhook", "")).strip()
    if not webhook:
        raise RuntimeError("notify.webhook is empty for wecom channel")

    markdown_content = _truncate_wecom_markdown(f"**{title}**\n\n{content}")
    body = {
        "msgtype": "markdown",
        "markdown": {
            "content": markdown_content,
        },
    }
    data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=15) as response:
        response_text = response.read().decode("utf-8", errors="replace")

    result = json.loads(response_text)
    payload = {
        "channel": "wecom",
        "title": title,
        "content": content,
        "status": "sent" if result.get("errcode") == 0 else "failed",
        "response": result,
    }
    if result.get("errcode") != 0:
        logger.error("push_message wecom failed: {}", payload)
        raise RuntimeError(f"wecom webhook failed: {result}")

    logger.info("push_message wecom sent: {}", payload)
    return payload


def _truncate_wecom_markdown(content: str) -> str:
    if len(content) <= WECOM_MARKDOWN_LIMIT:
        return content

    suffix = "\n\n...truncated..."
    allowed = max(WECOM_MARKDOWN_LIMIT - len(suffix), 0)
    return content[:allowed] + suffix
