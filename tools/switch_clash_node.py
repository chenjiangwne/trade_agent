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


def _resolve_target_node(item: dict, requested: str) -> str:
    all_nodes = [str(node) for node in item.get("all", [])]
    current = str(item.get("now", ""))
    req = str(requested or "").strip()
    if not all_nodes:
        raise RuntimeError("group has no selectable nodes")

    if req in all_nodes:
        return req

    req_lower = req.lower()
    auto_aliases = {"auto", "automatic", "auto_select", "auto-select", "自动", "自动选择", "♻️ 自动选择"}
    if req_lower in auto_aliases or req in auto_aliases:
        for node in all_nodes:
            node_lower = node.lower()
            if "自动" in node or "auto" in node_lower:
                return node
        if current and current in all_nodes:
            return current
        return all_nodes[0]

    # fuzzy match to tolerate terminal encoding/copy issues
    for node in all_nodes:
        if req and req in node:
            return node
    for node in all_nodes:
        if req_lower and req_lower in node.lower():
            return node

    if current and current in all_nodes:
        print(f"requested node not found in group, fallback to current node: {current}")
        return current
    raise RuntimeError(f"node not found in group: {requested}")


def switch_node(controller: str, secret: str, group: str, node: str) -> None:
    proxies = _get_proxies(controller, secret)
    item = proxies.get(group)
    if not item:
        raise RuntimeError(f"group not found: {group}")
    target = _resolve_target_node(item, node)
    _request(controller, secret, "PUT", f"/proxies/{quote(group, safe='')}", {"name": target})
    print(f"switched group={group} node={target}")


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
