from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


DEFAULT_CONTROLLER = "http://127.0.0.1:9097"
DEFAULT_SECRET = "fdasfasfdaddf"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _request(controller: str, secret: str, method: str, path: str, payload: dict | None = None) -> dict:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        f"{controller.rstrip('/')}{path}",
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {secret}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=10) as response:
            data = response.read().decode("utf-8")
            return json.loads(data) if data else {}
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Clash API error {exc.code}: {detail}") from exc


def _get_proxies(controller: str, secret: str) -> dict:
    return _request(controller, secret, "GET", "/proxies").get("proxies", {})


def list_groups(controller: str, secret: str) -> None:
    proxies = _get_proxies(controller, secret)
    for name, item in proxies.items():
        nodes = item.get("all")
        if not nodes:
            continue
        print(f"{name} | current={item.get('now', '')} | nodes={len(nodes)}")


def show_group(controller: str, secret: str, group: str) -> None:
    proxies = _get_proxies(controller, secret)
    item = proxies.get(group)
    if not item:
        raise RuntimeError(f"group not found: {group}")
    print(f"group={group}")
    print(f"current={item.get('now', '')}")
    for node in item.get("all", []):
        prefix = "*" if node == item.get("now") else " "
        print(f"{prefix} {node}")


def switch_node(controller: str, secret: str, group: str, node: str) -> None:
    _request(controller, secret, "PUT", f"/proxies/{quote(group, safe='')}", {"name": node})
    print(f"switched group={group} node={node}")


def main() -> None:
    parser = argparse.ArgumentParser(description="List or switch Clash/Mihomo proxy nodes.")
    parser.add_argument("--controller", default=DEFAULT_CONTROLLER)
    parser.add_argument("--secret", default=DEFAULT_SECRET)
    parser.add_argument("--list", action="store_true", help="List selectable proxy groups.")
    parser.add_argument("--group", help="Proxy group name.")
    parser.add_argument("--node", help="Node name to switch to.")
    args = parser.parse_args()

    if args.list:
        list_groups(args.controller, args.secret)
        return
    if args.group and not args.node:
        show_group(args.controller, args.secret, args.group)
        return
    if args.group and args.node:
        switch_node(args.controller, args.secret, args.group, args.node)
        return
    parser.error("use --list, --group GROUP, or --group GROUP --node NODE")


if __name__ == "__main__":
    main()
