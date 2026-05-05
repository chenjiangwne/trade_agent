from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generic.Common import yml_reader
from services.execution_service import _place_live_order, preview_live_order
from services.status_service import load_status


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview or place a Binance futures order for trade_agent.")
    parser.add_argument("--action", choices=["SHORT", "ADD_SHORT", "EXIT"], required=True)
    parser.add_argument("--live", action="store_true", help="Actually place the order. Default is dry-run preview.")
    args = parser.parse_args()

    config = yml_reader(str(PROJECT_ROOT / "config" / "config.yaml"))
    status = load_status(PROJECT_ROOT, config)
    decision = {
        "action": args.action,
        "score": 0.0,
        "bar_time": "",
        "reason": "manual demo",
        "status_updates": {},
    }

    if not args.live:
        print(preview_live_order(config, status, decision))
        return

    order = _place_live_order(config, status, decision)
    print(order)


if __name__ == "__main__":
    main()
