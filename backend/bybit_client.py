import hashlib
import hmac
import json
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any

import requests


class BybitClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.recv_window = "5000"

    def _sign(self, payload: str, timestamp: str) -> str:
        raw = f"{timestamp}{self.api_key}{self.recv_window}{payload}"
        return hmac.new(
            self.api_secret.encode("utf-8"),
            raw.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _headers(self, payload: str = "") -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": self._sign(payload, timestamp),
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: str = "",
        payload: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict:
        url = f"{self.base_url}{path}"
        body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""
        headers = self._headers(body if method == "POST" else params) if auth else None

        if method == "GET":
            if params:
                url = f"{url}?{params}"
            response = requests.get(url, headers=headers, timeout=10)
        else:
            response = requests.post(url, headers=headers, data=body, timeout=10)

        response.raise_for_status()
        return response.json()

    def get_ticker(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/v5/market/tickers",
            params=f"category=linear&symbol={symbol}",
        )

    def get_instrument_info(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/v5/market/instruments-info",
            params=f"category=linear&symbol={symbol}",
        )

    def get_balance(self) -> dict:
        return self._request(
            "GET",
            "/v5/account/wallet-balance",
            params="accountType=UNIFIED",
            auth=True,
        )

    def set_leverage(self, symbol: str, leverage: str) -> dict:
        return self._request(
            "POST",
            "/v5/position/set-leverage",
            payload={
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": leverage,
                "sellLeverage": leverage,
            },
            auth=True,
        )

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
        payload: dict[str, Any] = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": qty,
            "reduceOnly": reduce_only,
        }
        if order_type == "Limit":
            payload["price"] = price
            payload["timeInForce"] = "GTC"
        if order_link_id:
            payload["orderLinkId"] = order_link_id
        return self._request("POST", "/v5/order/create", payload=payload, auth=True)

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        return self._request(
            "POST",
            "/v5/order/cancel",
            payload={"category": "linear", "symbol": symbol, "orderId": order_id},
            auth=True,
        )

    def cancel_all_orders(self, symbol: str) -> dict:
        return self._request(
            "POST",
            "/v5/order/cancel-all",
            payload={"category": "linear", "symbol": symbol},
            auth=True,
        )

    def get_open_orders(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/v5/order/realtime",
            params=f"category=linear&symbol={symbol}&limit=50",
            auth=True,
        )

    def get_positions(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/v5/position/list",
            params=f"category=linear&symbol={symbol}",
            auth=True,
        )

    def get_order_history(self, symbol: str, limit: int = 50) -> dict:
        return self._request(
            "GET",
            "/v5/order/history",
            params=f"category=linear&symbol={symbol}&limit={limit}",
            auth=True,
        )

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
