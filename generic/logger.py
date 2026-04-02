import os
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger


def init_report(config, attempt):
    """Description: log file initialization"""
    logger.remove()
    if config["console"]["enabled"]:
        logger.add(
            sys.stderr,
            level=config["console"]["level"],
            format=config["console"]["format"],
        )

    if config.get("file", {}).get("enabled", True):
        repo_dir = Path(config["file"]["path"])
        repo_dir.mkdir(parents=True, exist_ok=True)
        current_time = datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d")
        log_file = repo_dir / f"trade_agent_{formatted_time}.log"

        logger.add(
            str(log_file),
            level=config["file"].get("level", "DEBUG"),
            rotation=config["file"].get("rotation", "10 MB"),
            retention=config["file"].get("retention", "7 days"),
            compression=config["file"].get("compression", "zip"),
            encoding="utf-8",
            enqueue=False,
            backtrace=True,
            diagnose=True,
        )

    logger.info(f"init Report initialized for Attempt {attempt}")
