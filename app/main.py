import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loguru import logger

from app.orchestrator import run_once
from generic.Common import yml_reader
from generic.logger import init_report
from services.market_data_service import seconds_until_next_4h_bar_by_exchange
from services.push_message import push_failure_message
from services.status_service import load_status


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
            result = run_once(project_root, config, force_process=(attempt == 1))
            logger.info("loop {} completed: {}", attempt, result)
            if attempt == 1:
                logger.info("initial run completed; next loop will follow exchange 4h schedule")
                attempt += 1
                continue
            wait_seconds = seconds_until_next_4h_bar_by_exchange(
                config,
                offset_seconds=config.get("runtime", {}).get("run_delay_seconds", 5),
            )
            logger.info("sleeping {} seconds until next 4h bar based on exchange clock", wait_seconds)
            attempt += 1
            time.sleep(wait_seconds)
    else:
        result = run_once(project_root, config, force_process=True)
        logger.info("trade_agent completed: {}", result)


if __name__ == "__main__":
    main()
