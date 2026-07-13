import argparse
import json
import sys
from decimal import Decimal, ROUND_DOWN
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[2] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402
from grid_engine import GridEngine  # noqa: E402


class ReplayClient:
    def __init__(self, case):
        self.market = case["market"]
        self.rules = case["rules"]

    def get_ticker(self, symbol):
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": symbol,
                        "lastPrice": self.market["last_price"],
                        "markPrice": self.market["mark_price"],
                    }
                ]
            },
        }

    def get_instrument_info(self, symbol):
        limit = self.rules["limit_quantity"]
        market = self.rules["market_quantity"]
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": symbol,
                        "priceFilter": {"tickSize": self.rules["tick_size"]},
                        "lotSizeFilter": {
                            "qtyStep": limit["step"],
                            "minOrderQty": limit["min"],
                            "maxOrderQty": limit["max"] or "0",
                            "minNotionalValue": self.rules["min_notional"],
                        },
                        "marketLotSizeFilter": {
                            "qtyStep": market["step"],
                            "minOrderQty": market["min"],
                            "maxOrderQty": market["max"] or "0",
                        },
                    }
                ]
            },
        }

    @staticmethod
    def round_to_step(value, step):
        value_decimal = Decimal(str(value))
        step_decimal = Decimal(str(step))
        rounded = (
            (value_decimal / step_decimal).quantize(Decimal("1"), rounding=ROUND_DOWN)
            * step_decimal
        )
        decimals = max(0, -step_decimal.as_tuple().exponent)
        return f"{rounded:.{decimals}f}"


def decimal_text(value):
    normalized = Decimal(str(value)).normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def replay_case(case):
    client = ReplayClient(case)
    config = main.GridConfig.model_validate(case["config"])
    direction = config.direction.lower().strip()
    grid_mode = config.grid_mode.lower().strip()
    preview = main._preview_grid(client, config, config.symbol, direction, grid_mode)

    engine = GridEngine(client, config.model_dump())
    engine._fetch_precision()
    engine.grid_levels = engine._calculate_levels()
    engine.current_price = float(case["market"]["last_price"])
    engine.current_mark_price = float(case["market"]["mark_price"])
    engine.initial_entry_price = float(preview["reference_price"])
    total_quantity = engine._prepare_pending_targets(float(preview["reference_price"]))
    plan = engine._validated_pending_target_plan()
    grid_time_in_force = "post_only" if config.grid_order_post_only else "gtc"

    opening_order = None
    if direction in {"long", "short"}:
        opening_order = {
            "side": "buy" if direction == "long" else "sell",
            "price": (
                None
                if config.initial_order_type == "market"
                else decimal_text(preview["reference_price"])
            ),
            "quantity": decimal_text(total_quantity),
            "kind": "market" if config.initial_order_type == "market" else "limit",
            "time_in_force": (
                "post_only" if config.initial_order_type == "post_only" else "gtc"
            ),
        }

    normalized_orders = [
        {
            "level_index": int(order["level_idx"]),
            "side": str(order["side"]).lower(),
            "price": decimal_text(order["price_text"]),
            "quantity": decimal_text(order["qty_text"]),
            "reduce_only": bool(order["reduce_only"]),
            "time_in_force": grid_time_in_force,
            "role": "profit" if order["reduce_only"] else "add",
        }
        for order in plan
    ]
    return {
        "name": case["name"],
        "plan": {
            "reference_price": decimal_text(preview["reference_price"]),
            "levels": [
                decimal_text(client.round_to_step(level, engine.tick_size))
                for level in engine.grid_levels
            ],
            "active_grid_count": int(preview["active_grid_count"]),
            "participating_level_count": len(
                {order["level_index"] for order in normalized_orders}
            ),
            "total_quantity": decimal_text(total_quantity),
            "opening_order": opening_order,
            "grid_orders": normalized_orders,
        },
    }


def replay_fixture(path):
    fixture = json.loads(path.read_text(encoding="utf-8"))
    if fixture.get("version") != 1:
        raise RuntimeError(f"unsupported replay fixture version {fixture.get('version')}")
    return {
        "version": 1,
        "results": [replay_case(case) for case in fixture["cases"]],
    }


def main_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("fixture", type=Path)
    arguments = parser.parse_args()
    print(
        json.dumps(
            replay_fixture(arguments.fixture),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main_cli()
