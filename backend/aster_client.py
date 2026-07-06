import copy
import json
import os
import threading
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data


class AsterFuturesClient:
    exchange = "aster"
    ASSET_PRICE_TTL_SECONDS = 60

    def __init__(
        self,
        user: str,
        signer_private_key: str,
        testnet: bool = False,
        *,
        signer: str | None = None,
        base_url: str | None = None,
        include_user_param: bool | None = None,
    ):
        self.user = str(user or "").strip()
        self.signer_private_key = str(signer_private_key or "").strip()
        self.testnet = testnet
        self.signer = str(signer or os.getenv("ASTER_SIGNER") or "").strip()
        if not self.signer and self.signer_private_key:
            self.signer = Account.from_key(self.signer_private_key).address

        self.include_user_param = (
            str(os.getenv("ASTER_INCLUDE_USER_PARAM", "true")).strip().lower() not in {"0", "false", "no", "off"}
            if include_user_param is None
            else bool(include_user_param)
        )
        self.base_url = (
            base_url
            or os.getenv("ASTER_BASE_URL")
            or (
                "https://fapi.asterdex.com"
                if not testnet
                else os.getenv("ASTER_TESTNET_BASE_URL", "https://fapi.asterdex-testnet.com")
            )
        ).rstrip("/")
        self.session = requests.Session()
        self._asset_price_cache: dict[str, tuple[Decimal, float]] = {}
        self._instrument_info_cache: dict[str, tuple[dict, float]] = {}
        self._nonce_lock = threading.Lock()
        self._last_nonce = 0

    def _nonce(self) -> int:
        with self._nonce_lock:
            nonce = int(time.time() * 1_000_000)
            if nonce <= self._last_nonce:
                nonce = self._last_nonce + 1
            self._last_nonce = nonce
            return nonce

    @staticmethod
    def _typed_data(message: str) -> dict:
        return {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [{"name": "msg", "type": "string"}],
            },
            "primaryType": "Message",
            "domain": {
                "name": "AsterSignTransaction",
                "version": "1",
                "chainId": 1666,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {"msg": message},
        }

    def _auth_params(self, params: dict[str, Any]) -> dict[str, Any]:
        if not self.signer_private_key:
            raise RuntimeError("Aster signer private key is required")
        if not self.signer:
            raise RuntimeError("Aster signer address is required")

        signed_params = dict(params)
        signed_params["nonce"] = str(self._nonce())
        if self.include_user_param and self.user:
            signed_params["user"] = self.user
        signed_params["signer"] = self.signer
        message = urlencode(signed_params, doseq=True)
        signable = encode_typed_data(full_message=self._typed_data(message))
        signature = Account.sign_message(signable, private_key=self.signer_private_key).signature.hex()
        signed_params["signature"] = signature
        return signed_params

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> Any:
        request_params = self._auth_params(params or {}) if auth else dict(params or {})
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "grid-trading/1.0"}
        request_kwargs: dict[str, Any] = {"headers": headers, "timeout": 10}
        if method.upper() == "GET":
            request_kwargs["params"] = request_params
        else:
            request_kwargs["data"] = request_params
        response = self.session.request(method, url, **request_kwargs)
        try:
            data = response.json()
        except ValueError:
            data = {"code": response.status_code, "msg": response.text}
        if response.status_code >= 400:
            message = data.get("msg") if isinstance(data, dict) else response.text
            raise RuntimeError(message or f"Aster request failed with {response.status_code}")
        if isinstance(data, dict) and data.get("code") not in (None, 0, "0", 200, "200"):
            raise RuntimeError(str(data.get("msg") or data))
        return data

    def get_ticker(self, symbol: str) -> dict:
        symbol = symbol.upper()
        ticker = self._request("GET", "/fapi/v3/ticker/24hr", params={"symbol": symbol})
        try:
            premium = self._request("GET", "/fapi/v3/premiumIndex", params={"symbol": symbol})
        except Exception:
            premium = {}
        price_change_pct = float(ticker.get("priceChangePercent", "0")) / 100
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": ticker.get("symbol", symbol),
                        "lastPrice": ticker.get("lastPrice") or ticker.get("price", "0"),
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
        cached = self._instrument_info_cache.get(symbol)
        now = time.time()
        if cached and now - cached[1] < 300:
            return cached[0]

        data = self._request("GET", "/fapi/v3/exchangeInfo", params={"symbol": symbol})
        instrument = next((item for item in data.get("symbols", []) if item.get("symbol") == symbol), None)
        if not instrument:
            return {"retCode": -1, "retMsg": f"Symbol {symbol} not found"}

        filters = {item.get("filterType"): item for item in instrument.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        result = {
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
        self._instrument_info_cache[symbol] = (result, now)
        return result

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
            "/fapi/v3/leverage",
            params={"symbol": symbol.upper(), "leverage": leverage},
            auth=True,
        )
        return {"retCode": 0}

    def _build_order_params(
        self,
        *,
        symbol: str,
        side: str,
        qty: str,
        price: str | None = None,
        order_type: str = "Limit",
        reduce_only: bool = False,
        order_link_id: str = "",
        time_in_force: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": self._to_aster_side(side),
            "type": order_type.upper(),
            "quantity": qty,
            "reduceOnly": "true" if reduce_only else "false",
        }
        if order_type.lower() == "limit":
            params["price"] = price
            params["timeInForce"] = "GTX" if time_in_force == "PostOnly" else (time_in_force or "GTC")
        if order_link_id:
            params["newClientOrderId"] = order_link_id
        return params

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
        time_in_force: str | None = None,
    ) -> dict:
        params = self._build_order_params(
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            order_type=order_type,
            reduce_only=reduce_only,
            order_link_id=order_link_id,
            time_in_force=time_in_force,
        )
        result = self._request("POST", "/fapi/v3/order", params=params, auth=True)
        return {"retCode": 0, "result": {"orderId": str(result.get("orderId", ""))}}

    def place_orders(self, orders: list[dict[str, Any]]) -> dict:
        batch_orders = []
        for order in orders:
            batch_orders.append(
                self._build_order_params(
                    symbol=str(order["symbol"]),
                    side=str(order["side"]),
                    qty=str(order["qty"]),
                    price=None if order.get("price") is None else str(order.get("price")),
                    order_type=str(order.get("order_type") or "Limit"),
                    reduce_only=bool(order.get("reduce_only", False)),
                    order_link_id=str(order.get("order_link_id") or ""),
                    time_in_force=order.get("time_in_force"),
                )
            )
        payload = json.dumps(batch_orders, separators=(",", ":"))
        results = self._request(
            "POST",
            "/fapi/v3/batchOrders",
            params={"batchOrders": payload},
            auth=True,
        )
        normalized = []
        for item in results if isinstance(results, list) else []:
            if isinstance(item, dict) and "orderId" in item:
                normalized.append({"retCode": 0, "result": {"orderId": str(item.get("orderId", ""))}})
            else:
                normalized.append(
                    {
                        "retCode": int(item.get("code", -1) or -1) if isinstance(item, dict) else -1,
                        "retMsg": item.get("msg", "Batch order failed") if isinstance(item, dict) else "Batch order failed",
                        "result": {},
                    }
                )
        return {"retCode": 0, "result": {"list": normalized}}

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v3/order",
            params={"symbol": symbol.upper(), "orderId": order_id},
            auth=True,
        )
        return {"retCode": 0}

    def cancel_all_orders(self, symbol: str) -> dict:
        self._request(
            "DELETE",
            "/fapi/v3/allOpenOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0}

    def get_open_orders(self, symbol: str) -> dict:
        orders = self._request(
            "GET",
            "/fapi/v3/openOrders",
            params={"symbol": symbol.upper()},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_order(item) for item in orders]}}

    def get_order(self, symbol: str, order_id: str) -> dict:
        order = self._request(
            "GET",
            "/fapi/v3/order",
            params={"symbol": symbol.upper(), "orderId": order_id},
            auth=True,
        )
        return {"retCode": 0, "result": self._normalize_order(order)}

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
            "/fapi/v3/allOrders",
            params={"symbol": symbol.upper(), "limit": limit},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_order(item) for item in orders]}}

    def get_order_trades(self, symbol: str, order_id: str) -> dict:
        trades = self._request(
            "GET",
            "/fapi/v3/userTrades",
            params={"symbol": symbol.upper(), "limit": 1000},
            auth=True,
        )
        trades = [item for item in trades if str(item.get("orderId", "")) == str(order_id)]
        return {"retCode": 0, "result": {"list": [self._normalize_trade(item) for item in trades]}}

    def get_recent_trades(self, symbol: str, limit: int = 100) -> dict:
        trades = self._request(
            "GET",
            "/fapi/v3/userTrades",
            params={"symbol": symbol.upper(), "limit": limit},
            auth=True,
        )
        return {"retCode": 0, "result": {"list": [self._normalize_trade(item) for item in trades]}}

    @staticmethod
    def _to_aster_side(side: str) -> str:
        return "BUY" if side.lower() == "buy" else "SELL"

    @staticmethod
    def _from_aster_side(side: str) -> str:
        return "Buy" if side.upper() == "BUY" else "Sell"

    def _normalize_order(self, item: dict[str, Any]) -> dict:
        return {
            "orderId": str(item.get("orderId", "")),
            "orderLinkId": item.get("clientOrderId", ""),
            "side": self._from_aster_side(str(item.get("side", ""))),
            "price": item.get("price", "0"),
            "qty": item.get("origQty", item.get("executedQty", "0")),
            "avgPrice": item.get("avgPrice", "0"),
            "executedQty": item.get("executedQty", "0"),
            "cumQuote": item.get("cumQuote", "0"),
            "orderStatus": item.get("status", ""),
            "reduceOnly": str(item.get("reduceOnly", "false")).lower() == "true" or item.get("reduceOnly") is True,
            "timeInForce": item.get("timeInForce", ""),
            "createdTime": str(item.get("time", item.get("updateTime", ""))),
        }

    def _normalize_trade(self, item: dict[str, Any]) -> dict:
        price = Decimal(str(item.get("price", "0")))
        qty = Decimal(str(item.get("qty", "0")))
        volume = Decimal(str(item.get("quoteQty") or (price * qty)))
        fee_amount = Decimal(str(item.get("commission", "0")))
        fee_asset = str(item.get("commissionAsset", "USDT")).upper()
        fee_usdt = self._fee_to_usdt(fee_amount, fee_asset)
        return {
            "orderId": str(item.get("orderId", "")),
            "tradeId": str(item.get("id", "")),
            "side": self._from_aster_side(str(item.get("side", ""))),
            "price": str(price),
            "qty": str(qty),
            "volume": str(volume),
            "fee": str(fee_amount),
            "feeAsset": fee_asset,
            "feeUsdt": "" if fee_usdt is None else str(fee_usdt),
            "realizedPnl": item.get("realizedPnl", "0"),
            "isMaker": bool(item.get("maker", False)),
            "time": str(item.get("time", "")),
        }

    def _fee_to_usdt(self, amount: Decimal, asset: str) -> Decimal | None:
        if amount == 0:
            return Decimal("0")
        if asset in {"USDT", "USDC", "BUSD", "FDUSD", "USD"}:
            return amount

        symbol = f"{asset}USDT"
        now = time.time()
        cached = self._asset_price_cache.get(symbol)
        if cached and now - cached[1] < self.ASSET_PRICE_TTL_SECONDS:
            return amount * cached[0]
        try:
            ticker = self._request("GET", "/fapi/v3/ticker/price", params={"symbol": symbol})
            price = Decimal(str(ticker.get("price", "0")))
            self._asset_price_cache[symbol] = (price, now)
            return amount * price
        except Exception:
            if cached:
                return amount * cached[0]
            return None

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

    def signature_payload(self, params: dict[str, Any]) -> str:
        signed_params = dict(params)
        signed_params["nonce"] = "1"
        if self.include_user_param and self.user:
            signed_params["user"] = self.user
        signed_params["signer"] = self.signer
        return urlencode(signed_params, doseq=True)

    def signature_typed_data(self, params: dict[str, Any]) -> dict:
        return copy.deepcopy(self._typed_data(self.signature_payload(params)))
