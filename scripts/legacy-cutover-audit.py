#!/usr/bin/env python3

"""Read-only cutover checks executed inside the legacy Python image."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import urlencode

import main


def emit(payload: dict, exit_code: int = 0) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    raise SystemExit(exit_code)


def state_audit() -> None:
    issues: list[str] = []
    engines = list(getattr(main, "_engines", {}).items())
    if engines:
        issues.append(f"legacy process still owns {len(engines)} in-memory engine(s)")

    integrity_fields = (
        "_api_config_integrity_error",
        "_grid_state_integrity_error",
        "_grid_history_integrity_error",
    )
    for field in integrity_fields:
        message = str(getattr(main, field, "") or "").strip()
        if message:
            issues.append(f"{field}: {message}")

    path = Path(getattr(main, "GRID_STATE_FILE", "/app/data/grid_state.json"))
    states: dict = {}
    try:
        if path.exists():
            document = json.loads(path.read_text(encoding="utf-8"))
            states = document.get("grids") or {}
            if not isinstance(states, dict):
                issues.append("legacy grid state has a non-object grids field")
                states = {}
    except Exception as error:  # Deployment must fail closed on unreadable legacy state.
        issues.append(f"legacy grid state cannot be read: {type(error).__name__}: {error}")

    running: list[str] = []
    active_order_counts: dict[str, int] = {}
    for key, value in states.items():
        if not isinstance(value, dict):
            issues.append(f"legacy grid state {key!r} is not an object")
            continue
        if value.get("running"):
            running.append(str(key))
        active_count = len(value.get("active_orders") or {})
        if active_count:
            active_order_counts[str(key)] = active_count
    if running:
        issues.append(f"legacy durable state still marks running grids: {', '.join(running)}")
    if active_order_counts:
        issues.append("legacy durable state still owns active order records")

    emit(
        {
            "safe": not issues,
            "engine_count": len(engines),
            "saved_grid_count": len(states),
            "running_grids": sorted(running),
            "active_order_counts": active_order_counts,
            "issues": issues,
        },
        0 if not issues else 2,
    )


def bybit_pages(client, path: str, parameters: dict[str, str]) -> list[dict]:
    rows: list[dict] = []
    cursor = ""
    seen: set[str] = set()
    for _ in range(100):
        page_parameters = dict(parameters)
        if cursor:
            page_parameters["cursor"] = cursor
        response = client._request(
            "GET",
            path,
            params=urlencode(page_parameters),
            auth=True,
        )
        if not isinstance(response, dict) or response.get("retCode") != 0:
            raise RuntimeError(f"Bybit {path} returned a non-success response")
        result = response.get("result") or {}
        page = result.get("list") or []
        if not isinstance(page, list):
            raise RuntimeError(f"Bybit {path} returned a non-list page")
        rows.extend(page)
        cursor = str(result.get("nextPageCursor") or "")
        if not cursor:
            return rows
        if cursor in seen:
            raise RuntimeError(f"Bybit {path} repeated a pagination cursor")
        seen.add(cursor)
    raise RuntimeError(f"Bybit {path} exceeded the pagination safety limit")


def exposure_audit() -> None:
    positions: list[dict] = []
    orders: list[dict] = []
    configured = sorted(getattr(main, "_clients", {}))
    try:
        for exchange in configured:
            client = main._clients[exchange]
            if exchange in {"binance", "aster"}:
                open_path = (
                    "/fapi/v1/openOrders" if exchange == "binance" else "/fapi/v3/openOrders"
                )
                position_rows = client._request(
                    "GET", "/fapi/v3/positionRisk", params={}, auth=True
                )
                order_rows = client._request("GET", open_path, params={}, auth=True)
                for row in position_rows or []:
                    quantity = str(row.get("positionAmt") or "0")
                    if float(quantity) == 0:
                        continue
                    positions.append(
                        {
                            "exchange": exchange,
                            "symbol": str(row.get("symbol") or ""),
                            "position_side": str(row.get("positionSide") or "BOTH"),
                            "quantity": quantity,
                            "entry_price": str(row.get("entryPrice") or "0"),
                        }
                    )
                for row in order_rows or []:
                    orders.append(
                        {
                            "exchange": exchange,
                            "symbol": str(row.get("symbol") or ""),
                            "order_id": str(row.get("orderId") or ""),
                            "client_order_id": str(row.get("clientOrderId") or ""),
                            "side": str(row.get("side") or ""),
                            "price": str(row.get("price") or "0"),
                            "quantity": str(row.get("origQty") or "0"),
                            "executed_quantity": str(row.get("executedQty") or "0"),
                            "reduce_only": bool(row.get("reduceOnly", False)),
                            "status": str(row.get("status") or ""),
                        }
                    )
            elif exchange == "bybit":
                position_rows = bybit_pages(
                    client,
                    "/v5/position/list",
                    {"category": "linear", "settleCoin": "USDT", "limit": "200"},
                )
                order_rows = bybit_pages(
                    client,
                    "/v5/order/realtime",
                    {
                        "category": "linear",
                        "settleCoin": "USDT",
                        "openOnly": "0",
                        "limit": "50",
                    },
                )
                for row in position_rows:
                    quantity = str(row.get("size") or "0")
                    if float(quantity) == 0:
                        continue
                    if row.get("side") == "Sell":
                        quantity = f"-{quantity}"
                    positions.append(
                        {
                            "exchange": exchange,
                            "symbol": str(row.get("symbol") or ""),
                            "position_side": str(row.get("side") or ""),
                            "quantity": quantity,
                            "entry_price": str(row.get("avgPrice") or "0"),
                        }
                    )
                for row in order_rows:
                    orders.append(
                        {
                            "exchange": exchange,
                            "symbol": str(row.get("symbol") or ""),
                            "order_id": str(row.get("orderId") or ""),
                            "client_order_id": str(row.get("orderLinkId") or ""),
                            "side": str(row.get("side") or ""),
                            "price": str(row.get("price") or "0"),
                            "quantity": str(row.get("qty") or "0"),
                            "executed_quantity": str(row.get("cumExecQty") or "0"),
                            "reduce_only": bool(row.get("reduceOnly", False)),
                            "status": str(row.get("orderStatus") or ""),
                        }
                    )
            else:
                raise RuntimeError(f"unsupported configured exchange: {exchange}")
    except Exception as error:
        emit(
            {
                "safe": False,
                "configured_exchanges": configured,
                "error": f"{type(error).__name__}: {error}",
            },
            3,
        )

    positions.sort(
        key=lambda row: (
            row["exchange"],
            row["symbol"],
            row["position_side"],
            row["quantity"],
        )
    )
    orders.sort(key=lambda row: (row["exchange"], row["symbol"], row["order_id"]))
    emit(
        {
            "safe": True,
            "configured_exchanges": configured,
            "position_count": len(positions),
            "open_order_count": len(orders),
            "positions": positions,
            "open_orders": orders,
        }
    )


def main_entry() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "state":
        state_audit()
    if mode == "exposure":
        exposure_audit()
    print("usage: legacy-cutover-audit.py state|exposure", file=sys.stderr)
    raise SystemExit(64)


if __name__ == "__main__":
    main_entry()
