from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loguru import logger

from app.orchestrator import run_once
from generic.Common import yml_reader
from generic.logger import init_report
from services.market_data_service import seconds_until_next_timeframe_bar_by_exchange
from services.push_message import push_failure_message, push_message
from services.status_service import load_status


def _sleep_with_heartbeat(
    config: dict,
    wait_seconds: int,
    heartbeat_seconds: int = 1800,
) -> None:
    remaining = max(int(wait_seconds), 0)
    if remaining <= 0:
        return

    heartbeat_seconds = max(int(heartbeat_seconds), 1)
    started_at = time.time()
    next_heartbeat_at = started_at + heartbeat_seconds

    while remaining > 0:
        now = time.time()
        if now >= next_heartbeat_at and remaining > 0:
            logger.info("heartbeat: waiting for next run, remaining={} seconds", remaining)
            push_message(
                config,
                title="trade_agent heartbeat",
                content=f"waiting for next run\nremaining_seconds={remaining}",
            )
            next_heartbeat_at += heartbeat_seconds

        sleep_chunk = min(remaining, max(1, int(next_heartbeat_at - now)))
        time.sleep(sleep_chunk)
        remaining = max(wait_seconds - int(time.time() - started_at), 0)


def main() -> None:
    project_root = PROJECT_ROOT
    config_path = project_root / "config" / "config.yaml"
    config = yml_reader(str(config_path))
    status = load_status(project_root, config)

    init_report(config["logging"], attempt=1)
    logger.info("trade_agent starting")

    if status.get("service_status") == "error":
        message = (
            f"startup blocked because status.service_status=error; "
            f"last_action={status.get('last_action')} "
            f"last_processed_1h_bar_time={status.get('last_processed_1h_bar_time')} "
            f"last_processed_4h_bar_time={status.get('last_processed_4h_bar_time')} "
            f"entry_price={status.get('entry_price')}"
        )
        logger.error(message)
        push_failure_message(
            config,
            title="trade_agent startup blocked by error status",
            content=message,
        )
        return

    if config.get("runtime", {}).get("run_forever", True):
        attempt = 1
        while True:
            logger.success("----------starting loop {}---------", attempt)
            try:
                result = run_once(project_root, config, force_process=(attempt == 1))
            except Exception as exc:
                error_detail = traceback.format_exc()
                push_failure_message(
                    config,
                    title="trade_agent runtime exception",
                    content=f"{exc}\n\n{error_detail}",
                )
                raise
            logger.info("loop {} completed: {}", attempt, result)
            if attempt == 1:
                logger.info("initial run completed; next loop will follow exchange 1h schedule")
                attempt += 1
                continue
            wait_seconds = seconds_until_next_timeframe_bar_by_exchange(
                config,
                timeframe=config["basic"]["timeframe_1h"],
                offset_seconds=config.get("runtime", {}).get("run_delay_seconds", 5),
            )
            logger.info(
                "sleeping {} seconds until next {} bar based on exchange clock",
                wait_seconds,
                config["basic"]["timeframe_1h"],
            )
            attempt += 1
            _sleep_with_heartbeat(config, wait_seconds)
    else:
        result = run_once(project_root, config, force_process=True)
        logger.info("trade_agent completed: {}", result)


if __name__ == "__main__":
    main()
