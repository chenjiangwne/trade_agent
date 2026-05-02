from __future__ import annotations

import json
from typing import Any
from urllib import request

from loguru import logger


def push_message(config: dict[str, Any], title: str, content: str) -> dict[str, Any]:
    channel = config.get("notify", {}).get("channel", "mock")
    payload: dict[str, Any] = {
        "channel": channel,
        "title": title,
        "content": content,
    }

    if channel != "wecom":
        payload["status"] = "mock_sent"
        logger.warning("push_message mock sent: {}", payload)
        return payload

    webhook = config.get("notify", {}).get("webhook", "")
    if not webhook:
        payload["status"] = "failed"
        payload["error"] = "notify.webhook is empty"
        logger.error("push_message failed: {}", payload)
        return payload

    body = json.dumps(
        {
            "msgtype": "text",
            "text": {"content": f"{title}\n\n{content}"},
        },
        ensure_ascii=False,
    ).encode("utf-8")

    req = request.Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    proxy = config.get("network", {}).get("proxy", "")
    opener = (
        request.build_opener(request.ProxyHandler({"http": proxy, "https": proxy}))
        if proxy
        else request.build_opener()
    )

    try:
        with opener.open(req, timeout=15) as response:
            response_text = response.read().decode("utf-8", errors="replace")
        response_data = json.loads(response_text)
    except Exception as exc:
        payload["status"] = "failed"
        payload["error"] = repr(exc)
        logger.error("push_message request failed: {}", payload)
        return payload

    payload["response"] = response_data
    if response_data.get("errcode") == 0:
        payload["status"] = "sent"
        logger.info("push_message sent: {}", payload)
    else:
        payload["status"] = "failed"
        logger.error("push_message rejected: {}", payload)
    return payload


def push_failure_message(config: dict[str, Any], title: str, content: str) -> dict[str, Any]:
    return push_message(config, title=title, content=content)
