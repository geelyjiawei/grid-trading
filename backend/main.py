import json
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from auth import (
    COOKIE_NAME,
    build_totp_uri,
    create_session,
    get_auth_settings,
    verify_password,
    verify_session,
    verify_totp,
)
from binance_client import BinanceFuturesClient
from bybit_client import BybitClient
from grid_engine import GridEngine
from secret_store import decrypt_text, encrypt_text, storage_backend


_engines: dict[str, GridEngine] = {}
SUPPORTED_EXCHANGES = {"bybit", "binance"}
DEFAULT_MAKER_FEE_RATE = float(os.getenv("GRID_MAKER_FEE_RATE", "0.0002"))
DEFAULT_TAKER_FEE_RATE = float(os.getenv("GRID_TAKER_FEE_RATE", "0.0005"))

BASE_DIR = os.path.dirname(__file__)
FRONTEND_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "frontend"))
API_CONFIG_FILE = os.getenv("GRID_CONFIG_FILE") or os.path.join(BASE_DIR, "api_config.json")
GRID_STATE_FILE = os.getenv("GRID_STATE_FILE") or os.path.join(os.path.dirname(API_CONFIG_FILE), "grid_state.json")
GRID_HISTORY_FILE = os.getenv("GRID_HISTORY_FILE") or os.path.join(os.path.dirname(API_CONFIG_FILE), "grid_history.json")


def _parse_bool(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _private_chmod(path: str):
    if os.name == "nt":
        return
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _cors_allowed_origins() -> list[str]:
    origins = os.getenv("CORS_ALLOWED_ORIGINS", "")
    return [item.strip() for item in origins.split(",") if item.strip()]


def _normalize_exchange(exchange: str | None) -> str:
    normalized = str(exchange or "bybit").strip().lower()
    return normalized if normalized in SUPPORTED_EXCHANGES else "bybit"


def _load_env_api_config() -> dict | None:
    exchange = _normalize_exchange(os.getenv("GRID_EXCHANGE") or os.getenv("EXCHANGE"))
    prefix = "BINANCE" if exchange == "binance" else "BYBIT"
    api_key = os.getenv(f"{prefix}_API_KEY", "").strip()
    api_secret = os.getenv(f"{prefix}_API_SECRET", "").strip()

    if not api_key and not api_secret and exchange == "bybit":
        binance_key = os.getenv("BINANCE_API_KEY", "").strip()
        binance_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        if binance_key and binance_secret:
            exchange = "binance"
            prefix = "BINANCE"
            api_key = binance_key
            api_secret = binance_secret

    if not api_key or not api_secret:
        return None

    return {
        "exchange": exchange,
        "api_key": api_key,
        "api_secret": api_secret,
        "testnet": _parse_bool(os.getenv(f"{prefix}_TESTNET")),
        "source": "env",
    }


def _load_api_config() -> dict:
    file_config = _load_file_api_config()
    if file_config and file_config.get("api_key") and file_config.get("api_secret"):
        return file_config

    env_config = _load_env_api_config()
    if env_config:
        return env_config

    return file_config or {"exchange": "bybit", "api_key": "", "api_secret": "", "testnet": False, "source": "none"}


def _load_file_api_config() -> dict | None:
    if not os.path.exists(API_CONFIG_FILE):
        return None

    try:
        with open(API_CONFIG_FILE, "r", encoding="utf-8") as file:
            config = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None

    try:
        if config.get("encrypted"):
            return {
                "api_key": decrypt_text(str(config.get("api_key", ""))),
                "api_secret": decrypt_text(str(config.get("api_secret", ""))),
                "exchange": _normalize_exchange(config.get("exchange")),
                "testnet": bool(config.get("testnet", False)),
                "source": "file",
            }

        # One-time migration for configs saved before encrypted storage existed.
        migrated = {
            "api_key": str(config.get("api_key", "")),
            "api_secret": str(config.get("api_secret", "")),
            "exchange": _normalize_exchange(config.get("exchange")),
            "testnet": bool(config.get("testnet", False)),
            "source": "file",
        }
        if migrated["api_key"] or migrated["api_secret"]:
            _save_api_config(migrated)
        return migrated
    except Exception:
        return None


def _save_api_config(config: dict):
    encrypted_config = {
        "encrypted": True,
        "backend": storage_backend(),
        "exchange": _normalize_exchange(config.get("exchange")),
        "api_key": encrypt_text(str(config.get("api_key", ""))),
        "api_secret": encrypt_text(str(config.get("api_secret", ""))),
        "testnet": bool(config.get("testnet", False)),
    }
    config_dir = os.path.dirname(API_CONFIG_FILE)
    if config_dir:
        os.makedirs(config_dir, exist_ok=True)
    with open(API_CONFIG_FILE, "w", encoding="utf-8") as file:
        json.dump(encrypted_config, file, ensure_ascii=False, indent=2)
    _private_chmod(API_CONFIG_FILE)


def _mask_api_key(api_key: str) -> str:
    return f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) >= 8 else api_key


def _build_client_from_config(config: dict):
    if not config.get("api_key") or not config.get("api_secret"):
        return None
    return _build_client(
        _normalize_exchange(config.get("exchange")),
        config["api_key"],
        config["api_secret"],
        bool(config.get("testnet", False)),
    )


def _build_client(exchange: str, api_key: str, api_secret: str, testnet: bool):
    if exchange == "binance":
        return BinanceFuturesClient(api_key, api_secret, testnet)
    return BybitClient(api_key, api_secret, testnet)


_api_config = _load_api_config()
_client = _build_client_from_config(_api_config)


def _load_grid_state_file() -> dict:
    if not os.path.exists(GRID_STATE_FILE):
        return {"version": 1, "grids": {}}

    try:
        with open(GRID_STATE_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "grids": {}}

    if not isinstance(state, dict):
        return {"version": 1, "grids": {}}
    state.setdefault("version", 1)
    state.setdefault("grids", {})
    return state


def _write_grid_state_file(state: dict):
    state_dir = os.path.dirname(GRID_STATE_FILE)
    if state_dir:
        os.makedirs(state_dir, exist_ok=True)
    tmp_path = f"{GRID_STATE_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)
    os.replace(tmp_path, GRID_STATE_FILE)
    _private_chmod(GRID_STATE_FILE)


def _load_grid_history_file() -> dict:
    if not os.path.exists(GRID_HISTORY_FILE):
        return {"version": 1, "runs": []}

    try:
        with open(GRID_HISTORY_FILE, "r", encoding="utf-8") as file:
            history = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "runs": []}

    if not isinstance(history, dict):
        return {"version": 1, "runs": []}
    history.setdefault("version", 1)
    history.setdefault("runs", [])
    return history


def _write_grid_history_file(history: dict):
    history_dir = os.path.dirname(GRID_HISTORY_FILE)
    if history_dir:
        os.makedirs(history_dir, exist_ok=True)
    tmp_path = f"{GRID_HISTORY_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as file:
        json.dump(history, file, ensure_ascii=False, indent=2)
    os.replace(tmp_path, GRID_HISTORY_FILE)
    _private_chmod(GRID_HISTORY_FILE)


def _history_record_from_engine(engine: GridEngine, status: str = "running") -> dict:
    config = engine.config
    return {
        "run_id": config.get("run_id", ""),
        "symbol": str(config.get("symbol", "")).upper(),
        "exchange": _normalize_exchange(_api_config.get("exchange")),
        "direction": config.get("direction", ""),
        "grid_mode": config.get("grid_mode", "arithmetic"),
        "lower_price": config.get("lower_price"),
        "upper_price": config.get("upper_price"),
        "grid_count": config.get("grid_count"),
        "leverage": config.get("leverage"),
        "total_investment": config.get("total_investment"),
        "status": status,
        "started_at": engine.start_time or time.time(),
        "updated_at": time.time(),
        "stopped_at": time.time() if status in {"stopped", "closed"} else None,
        "initial_side": engine.initial_side,
        "initial_qty": round(engine.initial_qty, 8),
        "completed_pairs": engine.completed_pairs,
        "gross_profit": round(engine.gross_profit, 4),
        "net_profit": round(engine.total_profit, 4),
        "total_fee": round(engine.total_fee, 4),
        "total_volume": round(engine.total_volume, 4),
        "filled_count": len(engine.filled_orders),
        "last_message": engine.trigger_message,
    }


def _round_down_steps(value: float, step: str) -> int:
    from decimal import Decimal, ROUND_DOWN

    value_decimal = Decimal(str(value))
    step_decimal = Decimal(str(step))
    return int((value_decimal / step_decimal).quantize(Decimal("1"), rounding=ROUND_DOWN))


def _steps_to_qty(steps: int, step: str) -> float:
    from decimal import Decimal

    return float(Decimal(str(step)) * Decimal(steps))


def _calculate_grid_levels(lower: float, upper: float, count: int, grid_mode: str) -> list[float]:
    if grid_mode == "geometric":
        ratio = (upper / lower) ** (1 / count)
        return [round(lower * (ratio ** idx), 10) for idx in range(count + 1)]

    step = (upper - lower) / count
    return [round(lower + (step * idx), 10) for idx in range(count + 1)]


def _validate_fee_rates(cfg: "GridConfig"):
    for field in ("fee_rate", "maker_fee_rate", "taker_fee_rate"):
        value = getattr(cfg, field)
        if value is not None and (value < 0 or value > 0.01):
            raise HTTPException(status_code=400, detail=f"{field} must be between 0 and 0.01")


def _preview_grid(client, cfg, symbol: str, direction: str, grid_mode: str) -> dict:
    ticker = client.get_ticker(symbol)
    if ticker.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=ticker.get("retMsg", "Failed to fetch current price"))
    current_price = float(ticker["result"]["list"][0]["lastPrice"])

    info_resp = client.get_instrument_info(symbol)
    if info_resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=info_resp.get("retMsg", "Failed to fetch instrument info"))
    instrument = info_resp["result"]["list"][0]
    qty_step = instrument["lotSizeFilter"]["qtyStep"]
    min_qty = float(instrument["lotSizeFilter"]["minOrderQty"])

    levels = _calculate_grid_levels(cfg.lower_price, cfg.upper_price, cfg.grid_count, grid_mode)
    if not (levels[0] < current_price < levels[-1]):
        raise HTTPException(
            status_code=400,
            detail=f"Current price {current_price} must stay inside the configured range {levels[0]} - {levels[-1]}",
        )

    if direction == "long":
        active_targets = [
            (idx, levels[idx + 1], "Sell")
            for idx in range(len(levels) - 1)
            if levels[idx + 1] > current_price
        ]
    elif direction == "short":
        active_targets = [
            (idx, levels[idx], "Buy")
            for idx in range(len(levels) - 1)
            if levels[idx] < current_price
        ]
    else:
        active_targets = [
            (idx, levels[idx], "Buy")
            for idx in range(len(levels) - 1)
            if levels[idx] < current_price
        ] + [
            (idx, levels[idx + 1], "Sell")
            for idx in range(len(levels) - 1)
            if levels[idx + 1] > current_price
        ]

    active_count = len(active_targets)
    if active_count <= 0:
        raise HTTPException(status_code=400, detail="No valid grid targets were found around current price")

    raw_total_qty = (cfg.total_investment * cfg.leverage) / current_price
    total_steps = _round_down_steps(raw_total_qty, qty_step)
    if total_steps < active_count:
        raise HTTPException(status_code=400, detail="Total investment is too small for this symbol and grid count")

    base_steps = total_steps // active_count
    remainder_steps = total_steps % active_count
    per_order_qtys = [
        _steps_to_qty(base_steps + (1 if index < remainder_steps else 0), qty_step)
        for index in range(active_count)
    ]
    total_qty = _steps_to_qty(total_steps, qty_step)

    if grid_mode == "geometric":
        ratio = (levels[1] / levels[0]) if len(levels) > 1 else 1
        grid_profit_pct = (ratio - 1) * 100
        grid_step = current_price * (ratio - 1)
    else:
        grid_step = levels[1] - levels[0] if len(levels) > 1 else 0
        grid_profit_pct = (grid_step / current_price) * 100 if current_price > 0 else 0

    average_qty = total_qty / active_count
    maker_fee_rate = max(0.0, float(cfg.maker_fee_rate if cfg.maker_fee_rate is not None else cfg.fee_rate or 0))
    taker_fee_rate = max(0.0, float(cfg.taker_fee_rate if cfg.taker_fee_rate is not None else cfg.fee_rate or 0))
    opening_fee_rate = maker_fee_rate if cfg.initial_order_type == "post_only" else taker_fee_rate
    grid_fee_rate = maker_fee_rate
    gross_profit = grid_step * average_qty
    open_fee = average_qty * current_price * opening_fee_rate
    close_fee = average_qty * current_price * grid_fee_rate
    fee = open_fee + close_fee

    return {
        "symbol": symbol,
        "current_price": current_price,
        "grid_step": grid_step,
        "grid_profit_pct": grid_profit_pct,
        "active_grid_count": active_count,
        "grid_count": cfg.grid_count,
        "total_qty": total_qty,
        "qty_per_grid_avg": average_qty,
        "qty_per_grid_min": min(per_order_qtys),
        "qty_per_grid_max": max(per_order_qtys),
        "qty_step": qty_step,
        "min_qty": min_qty,
        "per_grid_gross_profit": gross_profit,
        "per_grid_open_fee": open_fee,
        "per_grid_close_fee": close_fee,
        "per_grid_fee": fee,
        "per_grid_net_profit": gross_profit - fee,
        "maker_fee_rate": maker_fee_rate,
        "taker_fee_rate": taker_fee_rate,
    }


def _upsert_grid_history(engine: GridEngine, status: str = "running"):
    run_id = str(engine.config.get("run_id", ""))
    if not run_id:
        return

    history = _load_grid_history_file()
    runs = history.setdefault("runs", [])
    new_record = _history_record_from_engine(engine, status)
    for index, record in enumerate(runs):
        if record.get("run_id") == run_id:
            new_record["started_at"] = record.get("started_at") or new_record["started_at"]
            if status not in {"stopped", "closed"}:
                new_record["stopped_at"] = record.get("stopped_at")
            runs[index] = {**record, **new_record}
            break
    else:
        runs.append(new_record)

    history["runs"] = sorted(runs, key=lambda item: float(item.get("started_at") or 0), reverse=True)[:500]
    history["updated_at"] = time.time()
    _write_grid_history_file(history)


def _save_engine_state(engine: GridEngine):
    symbol = str(engine.config.get("symbol", "")).upper()
    if not symbol:
        return

    state = _load_grid_state_file()
    state["exchange"] = _normalize_exchange(_api_config.get("exchange"))
    state["testnet"] = bool(_api_config.get("testnet", False))
    state["updated_at"] = time.time()
    state.setdefault("grids", {})[symbol] = engine.to_state()
    _write_grid_state_file(state)
    _upsert_grid_history(engine, "running" if engine.running else "saved")


def _delete_engine_state(symbol: str):
    state = _load_grid_state_file()
    grids = state.setdefault("grids", {})
    grids.pop(symbol.upper(), None)
    state["updated_at"] = time.time()
    _write_grid_state_file(state)


def _restore_saved_engines():
    if not _client:
        return

    state = _load_grid_state_file()
    if _normalize_exchange(state.get("exchange")) != _normalize_exchange(_api_config.get("exchange")):
        return
    if bool(state.get("testnet", False)) != bool(_api_config.get("testnet", False)):
        return

    for symbol, engine_state in list(state.get("grids", {}).items()):
        if symbol in _engines:
            continue
        config = dict(engine_state.get("config") or {})
        if not config:
            continue
        config["symbol"] = str(config.get("symbol") or symbol).upper()
        engine = GridEngine(_client, config, state_callback=_save_engine_state)
        try:
            engine.restore_state(engine_state)
            _engines[config["symbol"]] = engine
            engine.start()
        except Exception:
            # Keep the saved state on disk so the UI/risk checks can still show the problem.
            _engines.pop(config["symbol"], None)


@asynccontextmanager
async def lifespan(_: FastAPI):
    _restore_saved_engines()
    yield
    for engine in list(_engines.values()):
        if engine.running:
            await engine.suspend()


app = FastAPI(title="Grid Trading", lifespan=lifespan)
_allowed_origins = _cors_allowed_origins()
if _allowed_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


AUTH_PUBLIC_PATHS = {"/api/auth/status", "/api/auth/login", "/api/auth/logout"}


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    settings = get_auth_settings()
    if (
        not settings.required
        or not request.url.path.startswith("/api/")
        or request.url.path in AUTH_PUBLIC_PATHS
    ):
        return await call_next(request)

    if not settings.configured:
        return JSONResponse(
            status_code=503,
            content={"detail": "Authentication is required but not configured"},
        )

    username = verify_session(request.cookies.get(COOKIE_NAME, ""), settings)
    if not username:
        return JSONResponse(status_code=401, content={"detail": "Authentication required"})

    request.state.auth_user = username
    return await call_next(request)


class ApiConfig(BaseModel):
    exchange: str = "bybit"
    api_key: str = Field(min_length=1)
    api_secret: str = Field(min_length=1)
    testnet: bool = False


class GridConfig(BaseModel):
    symbol: str
    direction: str
    upper_price: float
    lower_price: float
    grid_count: int
    total_investment: float
    leverage: int
    fee_rate: float = DEFAULT_TAKER_FEE_RATE
    maker_fee_rate: float | None = DEFAULT_MAKER_FEE_RATE
    taker_fee_rate: float | None = DEFAULT_TAKER_FEE_RATE
    initial_order_type: str = "market"
    initial_order_price: float | None = None
    grid_order_post_only: bool = False
    grid_mode: str = "arithmetic"
    trigger_price: float | None = None
    stop_loss_price: float | None = None
    take_profit_price: float | None = None


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    code: str = Field(min_length=6, max_length=12)


@app.get("/")
def index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


@app.get("/api/auth/status")
def auth_status(request: Request):
    settings = get_auth_settings()
    username = None
    if settings.configured:
        username = verify_session(request.cookies.get(COOKIE_NAME, ""), settings)

    response = {
        "required": settings.required,
        "configured": settings.configured,
        "authenticated": bool(username) or not settings.required,
        "username": username,
    }
    if (
        settings.required
        and settings.configured
        and not username
        and _parse_bool(os.getenv("AUTH_SHOW_TOTP_SETUP"))
    ):
        response["totp_uri"] = build_totp_uri(settings)
        response["totp_secret"] = settings.totp_secret
    return response


@app.post("/api/auth/login")
def auth_login(payload: LoginRequest, response: Response):
    settings = get_auth_settings()
    if not settings.required:
        return {"ok": True, "message": "Authentication is disabled"}
    if not settings.configured:
        raise HTTPException(status_code=503, detail="Authentication is required but not configured")
    if payload.username != settings.username:
        raise HTTPException(status_code=401, detail="Invalid username, password, or code")
    if not verify_password(payload.password, settings.password_hash):
        raise HTTPException(status_code=401, detail="Invalid username, password, or code")
    if not verify_totp(payload.code, settings.totp_secret):
        raise HTTPException(status_code=401, detail="Invalid username, password, or code")

    token = create_session(settings.username, settings)
    response.set_cookie(
        COOKIE_NAME,
        token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        max_age=43200,
    )
    return {"ok": True, "message": "Logged in"}


@app.post("/api/auth/logout")
def auth_logout(response: Response):
    response.delete_cookie(COOKIE_NAME)
    return {"ok": True, "message": "Logged out"}


@app.get("/api/config")
def get_config():
    api_key = _api_config.get("api_key", "")
    return {
        "exchange": _normalize_exchange(_api_config.get("exchange")),
        "api_key": _mask_api_key(api_key),
        "testnet": _api_config.get("testnet", False),
        "configured": bool(api_key),
        "source": _api_config.get("source", "none"),
        "storage": storage_backend(),
    }


@app.post("/api/config")
def set_config(cfg: ApiConfig):
    global _client, _api_config

    if any(engine.running for engine in _engines.values()):
        raise HTTPException(status_code=400, detail="Stop all running grids before changing exchange API config")

    exchange = _normalize_exchange(cfg.exchange)
    candidate = _build_client(exchange, cfg.api_key.strip(), cfg.api_secret.strip(), cfg.testnet)
    try:
        balance = candidate.get_balance()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to connect to {exchange.title()}: {exc}") from exc

    if balance.get("retCode") != 0:
        raise HTTPException(
            status_code=400,
            detail=f"API verification failed: {balance.get('retMsg', 'unknown error')}",
        )

    saved_config = cfg.model_dump()
    saved_config["exchange"] = exchange
    try:
        _save_api_config(saved_config)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=f"API verified but not saved securely: {exc}") from exc

    _client = candidate
    _api_config = {**saved_config, "source": "file"}
    return {"ok": True, "message": f"{exchange.title()} API config saved"}


@app.get("/api/price/{symbol}")
def get_price(symbol: str):
    exchange = _normalize_exchange(_api_config.get("exchange"))
    client = _client or _build_client(exchange, "", "", bool(_api_config.get("testnet", False)))
    resp = client.get_ticker(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch ticker"))

    data = resp["result"]["list"][0]
    return {
        "symbol": data["symbol"],
        "last_price": data["lastPrice"],
        "index_price": data.get("indexPrice", ""),
        "mark_price": data.get("markPrice", ""),
        "price_24h_pcnt": data.get("price24hPcnt", "0"),
        "volume_24h": data.get("volume24h", "0"),
    }


@app.get("/api/balance")
def get_balance():
    client = _get_client()
    resp = client.get_balance()
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch balance"))

    wallets = resp["result"]["list"][0].get("coin", [])
    usdt = next((item for item in wallets if item.get("coin") == "USDT"), None)
    if not usdt:
        return {"available": "0", "equity": "0", "unrealised_pnl": "0"}

    return {
        "available": usdt.get("availableToWithdraw") or usdt.get("walletBalance", "0"),
        "equity": usdt.get("equity", "0"),
        "unrealised_pnl": usdt.get("unrealisedPnl", "0"),
    }


@app.get("/api/positions/{symbol}")
def get_positions(symbol: str):
    client = _get_client()
    resp = client.get_positions(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch positions"))

    positions = []
    for item in resp["result"].get("list", []):
        try:
            size = float(item.get("size", 0))
        except (TypeError, ValueError):
            size = 0

        if size <= 0:
            continue

        positions.append(
            {
                "side": item.get("side", ""),
                "size": item.get("size", "0"),
                "entry_price": item.get("avgPrice", "0"),
                "mark_price": item.get("markPrice", "0"),
                "unrealised_pnl": item.get("unrealisedPnl", "0"),
                "leverage": item.get("leverage", ""),
                "liq_price": item.get("liqPrice", ""),
            }
        )

    return {"positions": positions}


@app.post("/api/grid/preview")
def grid_preview(cfg: GridConfig):
    client = _get_client()
    symbol = cfg.symbol.upper().strip()
    direction = cfg.direction.lower().strip()
    grid_mode = cfg.grid_mode.lower().strip()

    if cfg.upper_price <= cfg.lower_price:
        raise HTTPException(status_code=400, detail="upper_price must be greater than lower_price")
    if cfg.grid_count < 2 or cfg.grid_count > 100:
        raise HTTPException(status_code=400, detail="grid_count must be between 2 and 100")
    if cfg.total_investment <= 0:
        raise HTTPException(status_code=400, detail="total_investment must be greater than 0")
    if cfg.leverage < 1 or cfg.leverage > 125:
        raise HTTPException(status_code=400, detail="leverage must be between 1 and 125")
    _validate_fee_rates(cfg)
    if direction not in {"long", "short", "neutral"}:
        raise HTTPException(status_code=400, detail="direction must be long, short, or neutral")
    if grid_mode not in {"arithmetic", "geometric"}:
        raise HTTPException(status_code=400, detail="grid_mode must be arithmetic or geometric")

    return _preview_grid(client, cfg, symbol, direction, grid_mode)


@app.post("/api/grid/start")
async def start_grid(cfg: GridConfig):
    client = _get_client()
    symbol = cfg.symbol.upper().strip()
    direction = cfg.direction.lower().strip()
    grid_mode = cfg.grid_mode.lower().strip()
    initial_order_type = cfg.initial_order_type.lower().strip()
    existing_engine = _engines.get(symbol)

    if existing_engine and existing_engine.running:
        raise HTTPException(status_code=400, detail=f"A grid is already running for {symbol}")
    if cfg.upper_price <= cfg.lower_price:
        raise HTTPException(status_code=400, detail="upper_price must be greater than lower_price")
    if cfg.grid_count < 2 or cfg.grid_count > 100:
        raise HTTPException(status_code=400, detail="grid_count must be between 2 and 100")
    if cfg.total_investment <= 0:
        raise HTTPException(status_code=400, detail="total_investment must be greater than 0")
    if cfg.leverage < 1 or cfg.leverage > 125:
        raise HTTPException(status_code=400, detail="leverage must be between 1 and 125")
    _validate_fee_rates(cfg)
    if direction not in {"long", "short", "neutral"}:
        raise HTTPException(status_code=400, detail="direction must be long, short, or neutral")
    if grid_mode not in {"arithmetic", "geometric"}:
        raise HTTPException(status_code=400, detail="grid_mode must be arithmetic or geometric")
    if initial_order_type not in {"market", "post_only"}:
        raise HTTPException(status_code=400, detail="initial_order_type must be market or post_only")
    if direction == "neutral" and initial_order_type != "market":
        raise HTTPException(status_code=400, detail="post_only initial order is only supported for long or short grids")
    if cfg.initial_order_price is not None and cfg.initial_order_price <= 0:
        raise HTTPException(status_code=400, detail="initial_order_price must be greater than 0")

    engine_config = cfg.model_dump()
    engine_config["symbol"] = symbol
    engine_config["direction"] = direction
    engine_config["grid_mode"] = grid_mode
    engine_config["initial_order_type"] = initial_order_type
    engine_config["run_id"] = f"{symbol}_{int(time.time())}_{os.urandom(3).hex()}"
    engine = GridEngine(client, engine_config, state_callback=_save_engine_state)

    try:
        await engine.initialize()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _engines[symbol] = engine
    engine.start()
    _save_engine_state(engine)
    return {"ok": True, "message": f"{symbol} {direction} grid started"}


@app.post("/api/grid/stop")
async def stop_grid():
    running_symbols = [symbol for symbol, engine in _engines.items() if engine.running]
    if not running_symbols:
        raise HTTPException(status_code=400, detail="No active grid")
    if len(running_symbols) > 1:
        raise HTTPException(status_code=400, detail="Multiple grids are running; stop by symbol")

    return await _stop_grid(running_symbols[0])


@app.post("/api/grid/stop/{symbol}")
async def stop_grid_symbol(symbol: str):
    return await _stop_grid(symbol.upper().strip())


@app.post("/api/grid/stop-all")
async def stop_all_grids():
    running = [engine for engine in _engines.values() if engine.running]
    if not running:
        raise HTTPException(status_code=400, detail="No active grid")

    for engine in running:
        symbol = str(engine.config.get("symbol", "")).upper()
        await engine.stop()
        _upsert_grid_history(engine, "stopped")
        if symbol:
            _delete_engine_state(symbol)

    return {"ok": True, "message": "All grids stopped and open orders cancelled"}


async def _stop_grid(symbol: str):
    engine = _engines.get(symbol)
    if not engine or not engine.running:
        raise HTTPException(status_code=400, detail="No active grid")

    await engine.stop()
    _upsert_grid_history(engine, "stopped")
    _delete_engine_state(symbol)
    return {"ok": True, "message": f"{symbol} grid stopped and open orders cancelled"}


@app.get("/api/grid/status")
def grid_status():
    statuses = [_engine_status(engine) for engine in _engines.values()]
    return {
        "running": any(status["running"] for status in statuses),
        "engine_count": len(statuses),
        "running_count": sum(1 for status in statuses if status["running"]),
        "engines": statuses,
    }


@app.get("/api/grid/status/{symbol}")
def grid_symbol_status(symbol: str):
    symbol = symbol.upper().strip()
    engine = _engines.get(symbol)
    if not engine:
        return {"running": False, "symbol": symbol}
    return _engine_status(engine)


def _engine_status(engine: GridEngine) -> dict:
    status = engine.get_status()
    unrealised_pnl = 0.0
    try:
        client = _get_client()
        resp = client.get_positions(str(status.get("symbol", "")).upper())
        if resp.get("retCode") == 0:
            for item in resp.get("result", {}).get("list", []):
                try:
                    unrealised_pnl += float(item.get("unrealisedPnl", 0) or 0)
                except (TypeError, ValueError):
                    continue
    except Exception:
        unrealised_pnl = 0.0

    realized_net = float(status.get("total_profit", 0) or 0)
    status["realized_gross_profit"] = status.get("gross_profit", 0)
    status["realized_net_profit"] = round(realized_net, 4)
    status["unrealised_pnl"] = round(unrealised_pnl, 4)
    status["total_equity_profit"] = round(realized_net + unrealised_pnl, 4)
    return status


@app.get("/api/grid/history")
def grid_history(limit: int = 100):
    history = _load_grid_history_file()
    safe_limit = max(1, min(int(limit or 100), 500))
    runs = sorted(
        history.get("runs", []),
        key=lambda item: float(item.get("started_at") or 0),
        reverse=True,
    )[:safe_limit]
    return {"runs": runs}


def _is_grid_order(item: dict) -> bool:
    link_id = str(item.get("orderLinkId") or item.get("order_link_id") or "")
    return link_id.startswith(("g_", "open_"))


def _managed_order_ids(engine: GridEngine | None) -> set[str]:
    if not engine:
        return set()
    ids = {str(order.get("order_id", "")) for order in engine.active_orders.values()}
    if engine.opening_order:
        ids.add(str(engine.opening_order.get("order_id", "")))
    return {order_id for order_id in ids if order_id}


def _risk_snapshot(symbol: str) -> dict:
    client = _get_client()
    symbol = symbol.upper().strip()
    engine = _engines.get(symbol)
    managed_ids = _managed_order_ids(engine)

    open_resp = client.get_open_orders(symbol)
    if open_resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=open_resp.get("retMsg", "Failed to fetch open orders"))
    open_orders = open_resp["result"].get("list", [])
    grid_orders = [order for order in open_orders if _is_grid_order(order)]
    orphan_orders = [
        {
            "order_id": item.get("orderId", ""),
            "order_link_id": item.get("orderLinkId", ""),
            "side": item.get("side", ""),
            "price": item.get("price", "0"),
            "qty": item.get("qty", "0"),
            "status": item.get("orderStatus", ""),
            "reduce_only": item.get("reduceOnly", False),
            "created_time": item.get("createdTime", ""),
        }
        for item in grid_orders
        if str(item.get("orderId", "")) not in managed_ids
    ]

    position_resp = client.get_positions(symbol)
    if position_resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=position_resp.get("retMsg", "Failed to fetch positions"))
    positions = []
    for item in position_resp["result"].get("list", []):
        try:
            size = float(item.get("size", 0))
        except (TypeError, ValueError):
            size = 0
        if size > 0:
            positions.append(
                {
                    "side": item.get("side", ""),
                    "size": item.get("size", "0"),
                    "entry_price": item.get("avgPrice", "0"),
                    "mark_price": item.get("markPrice", "0"),
                    "unrealised_pnl": item.get("unrealisedPnl", "0"),
                }
            )

    unmanaged_position = bool(positions and (not engine or not engine.running))
    return {
        "symbol": symbol,
        "engine_running": bool(engine and engine.running),
        "orphan_order_count": len(orphan_orders),
        "orphan_orders": orphan_orders,
        "unmanaged_position": unmanaged_position,
        "positions": positions,
        "has_risk": bool(orphan_orders or unmanaged_position),
    }


@app.get("/api/risk/{symbol}")
def risk_snapshot(symbol: str):
    return _risk_snapshot(symbol)


@app.post("/api/risk/cancel-orphans/{symbol}")
def cancel_orphan_orders(symbol: str):
    client = _get_client()
    snapshot = _risk_snapshot(symbol)
    cancelled = []
    for order in snapshot["orphan_orders"]:
        order_id = str(order.get("order_id", ""))
        if not order_id:
            continue
        resp = client.cancel_order(snapshot["symbol"], order_id)
        if resp.get("retCode") != 0:
            raise HTTPException(status_code=400, detail=resp.get("retMsg", f"Failed to cancel {order_id}"))
        cancelled.append(order_id)
    return {"ok": True, "symbol": snapshot["symbol"], "cancelled": cancelled}


@app.get("/api/orders/history/{symbol}")
def order_history(symbol: str):
    client = _get_client()
    resp = client.get_order_history(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch order history"))

    orders = [
        {
            "order_id": item.get("orderId", ""),
            "side": item.get("side", ""),
            "price": item.get("price", "0"),
            "qty": item.get("qty", "0"),
            "status": item.get("orderStatus", ""),
            "created_time": item.get("createdTime", ""),
        }
        for item in resp["result"].get("list", [])
    ]
    return {"orders": orders}


@app.get("/api/orders/open/{symbol}")
def open_orders(symbol: str):
    client = _get_client()
    resp = client.get_open_orders(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch open orders"))

    orders = [
        {
            "order_id": item.get("orderId", ""),
            "order_link_id": item.get("orderLinkId", ""),
            "side": item.get("side", ""),
            "price": item.get("price", "0"),
            "qty": item.get("qty", "0"),
            "status": item.get("orderStatus", ""),
            "reduce_only": item.get("reduceOnly", False),
            "created_time": item.get("createdTime", ""),
        }
        for item in resp["result"].get("list", [])
    ]
    return {"orders": orders}


@app.post("/api/orders/cancel-all/{symbol}")
def cancel_all_symbol_orders(symbol: str):
    client = _get_client()
    resp = client.cancel_all_orders(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to cancel open orders"))
    return {"ok": True, "message": f"All open orders for {symbol.upper()} were cancelled"}


def _get_client():
    if not _client:
        raise HTTPException(status_code=400, detail="Please configure API Key first")
    return _client


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
