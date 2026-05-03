import hashlib
import hmac
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import requests


class BinanceFuturesClient:
    exchange = "binance"

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        self.recv_window = 5000

    def _sign(self, params: dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> Any:
        request_params = dict(params or {})
        headers = {"X-MBX-APIKEY": self.api_key} if auth else None

        if auth:
            request_params["timestamp"] = int(time.time() * 1000)
            request_params["recvWindow"] = self.recv_window
            request_params["signature"] = self._sign(request_params)

        url = f"{self.base_url}{path}"
        response = requests.request(method, url, params=request_params, headers=headers, timeout=10)
        try:
            data = response.json()
        except ValueError:
            data = {"code": response.status_code, "msg": response.text}
        if response.status_code >= 400:
            message = data.get("msg") if isinstance(data, dict) else response.text
            raise RuntimeError(message or f"Binance request failed with {response.status_code}")
        return data

    def get_ticker(self, symbol: str) -> dict:
        symbol = symbol.upper()
        ticker = self._request("GET", "/fapi/v1/ticker/24hr", params={"symbol": symbol})
        premium = self._request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol})
        price_change_pct = float(ticker.get("priceChangePercent", "0")) / 100
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": ticker.get("symbol", symbol),
                        "lastPrice": ticker.get("lastPrice", "0"),
                        "indexPrice": premium.get("indexPrice", ""),
                        "markPrice": premium.get("markPrice", ""),
                        "price24hPcnt": str(price_change_pct),
                        "volume24h": ticker.get("quoteVolume") or ticker.get("volume", "0"),
                    }
                ]
            },
        }

    def get_instrument_info(self, symbol: str) -> dict:
        symbol = symbol.upper()
        data = self._request("GET", "/fapi/v1/exchangeInfo")
        instrument = next((item for item in data.get("symbols", []) if item.get("symbol") == symbol), None)
        if not instrument:
            return {"retCode": -1, "retMsg": f"Symbol {symbol} not found"}

        filters = {item.get("filterType"): item for item in instrument.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "priceFilter": {"tickSize": price_filter.get("tickSize", "0.1")},
                        "lotSizeFilter": {
                            "qtyStep": lot_filter.get("stepSize", "0.001"),
                            "minOrderQty": lot_filter.get("minQty", "0.001"),
                        },
                    }
                ]
            },
        }

    def get_balance(self) -> dict:
        balances = self._request("GET", "/fapi/v3/balance", auth=True)
        usdt = next((item for item in balances if item.get("asset") == "USDT"), {})
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "availableToWithdraw": usdt.get("availableBalance", "0"),
                                "walletBalance": usdt.get("balance", "0"),
                                "equity": usdt.get("balance", "0"),
                                "unrealisedPnl": usdt.get("crossUnPnl", "0"),
                            }
                        ]
                    }
                ]
            },
        }

    def set_leverage(self, symbol: str, leverage: str) -> dict:
        self._request(
            "POST",
            "/fapi/v1/leverage",
            params={"symbol": symbol.upper(), "leverage": leverage},
            auth=True,
        )
        return {"retCode": 0}

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: str,
        price: str | None = None,
        order_type: str = "Limit",
        reduce_only: bool = False,
        order_link_id: str = "",
    ) -> dict:
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": self._to_binance_side(side),
            "type": order_type.upper(),
            "quantity": qty,
            "reduceOnly": "true" if reduce_only else "false",
        }
        if order_type.lower() == "limit":
            params["price"] = price
            params["timeInForce"] = "GTC"
        if order_link_id:
            params["newClientOrderId"] = order_link_id

        result = self._request("POST", "/fapi/v1/order", params=params, auth=True)
        return {"retCode": 0, "result": {"orderId": str(result.get("orderId", ""))}}

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v1/order",
            params={"symbol": symbol.upper(), "orderId": order_id},
            auth=True,
        )
        return {"retCode": 0}

    def cancel_all_orders(self, symbol: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0}

    def get_open_orders(self, symbol: str) -> dict:
        orders = self._request(
            "GET",
            "/fapi/v1/openOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {"list": [self._normalize_order(item) for item in orders]},
        }

    def get_positions(self, symbol: str) -> dict:
        positions = self._request(
            "GET",
            "/fapi/v3/positionRisk",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_position(item) for item in positions]}}

    def get_order_history(self, symbol: str, limit: int = 50) -> dict:
        orders = self._request(
            "GET",
            "/fapi/v1/allOrders",
            params={"symbol": symbol.upper(), "limit": limit},
            auth=True,
        )
        return {
            "retCode": 0,
            "result": {"list": [self._normalize_order(item) for item in orders]},
        }

    @staticmethod
    def _to_binance_side(side: str) -> str:
        return "BUY" if side.lower() == "buy" else "SELL"

    @staticmethod
    def _from_binance_side(side: str) -> str:
        return "Buy" if side.upper() == "BUY" else "Sell"

    def _normalize_order(self, item: dict[str, Any]) -> dict:
        return {
            "orderId": str(item.get("orderId", "")),
            "orderLinkId": item.get("clientOrderId", ""),
            "side": self._from_binance_side(str(item.get("side", ""))),
            "price": item.get("price", "0"),
            "qty": item.get("origQty", item.get("executedQty", "0")),
            "orderStatus": item.get("status", ""),
            "reduceOnly": item.get("reduceOnly", False),
            "createdTime": str(item.get("time", "")),
        }

    @staticmethod
    def _normalize_position(item: dict[str, Any]) -> dict:
        amount = Decimal(str(item.get("positionAmt", "0")))
        side = "Buy" if amount > 0 else "Sell"
        size = abs(amount)
        return {
            "side": side,
            "size": str(size.normalize()) if size else "0",
            "avgPrice": item.get("entryPrice", "0"),
            "markPrice": item.get("markPrice", "0"),
            "unrealisedPnl": item.get("unRealizedProfit", "0"),
            "leverage": item.get("leverage", ""),
            "liqPrice": item.get("liquidationPrice", ""),
        }

    @staticmethod
    def round_to_step(value: float, step: str) -> str:
        step_decimal = Decimal(step)
        value_decimal = Decimal(str(value))
        rounded = (
            (value_decimal / step_decimal).quantize(Decimal("1"), rounding=ROUND_DOWN)
            * step_decimal
        )
        decimals = max(0, -step_decimal.as_tuple().exponent)
        return f"{rounded:.{decimals}f}"
