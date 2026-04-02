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
from services.market_data_service import seconds_until_next_4h_bar


def main() -> None:
    project_root = PROJECT_ROOT
    config_path = project_root / "config" / "config.yaml"
    config = yml_reader(str(config_path))

    init_report(config["logging"], attempt=1)
    logger.info("trade_agent starting")

    if config.get("runtime", {}).get("run_forever", True):
        attempt = 1
        while True:
            logger.info("starting loop {}", attempt)
            result = run_once(project_root, config)
            logger.info("loop {} completed: {}", attempt, result)
            wait_seconds = seconds_until_next_4h_bar(offset_seconds=config.get("runtime", {}).get("run_delay_seconds", 5))
            logger.info("sleeping {} seconds until next 4h bar", wait_seconds)
            attempt += 1
            time.sleep(wait_seconds)
    else:
        result = run_once(project_root, config)
        logger.info("trade_agent completed: {}", result)


if __name__ == "__main__":
    main()
