import asyncio
import json
import logging
import os
import tempfile
import time
from contextlib import asynccontextmanager
from itertools import pairwise
from threading import RLock

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
from aster_client import AsterFuturesClient
from binance_client import BinanceFuturesClient
from bybit_client import BybitClient
from fee_rates import normalize_fee_rate
from grid_engine import GridEngine
from secret_store import decrypt_text, encrypt_text, storage_backend


logger = logging.getLogger(__name__)
_engines: dict[str, GridEngine] = {}
_engines_lock = RLock()
_state_files_lock = RLock()
_config_file_lock = RLock()
_starting_engine_keys: set[str] = set()
SUPPORTED_EXCHANGES = {"bybit", "binance", "aster"}
DEFAULT_EXCHANGE = "bybit"
DEFAULT_MAKER_FEE_RATE = float(os.getenv("GRID_MAKER_FEE_RATE", "0.0002"))
DEFAULT_TAKER_FEE_RATE = float(os.getenv("GRID_TAKER_FEE_RATE", "0.0005"))
HISTORY_STATE_UPDATE_INTERVAL_SECONDS = 1.0


class GridStateIntegrityError(RuntimeError):
    """Raised when the durable trading ledger cannot be read safely."""


class GridHistoryIntegrityError(RuntimeError):
    """Raised when strategy history cannot be read without risking data loss."""


class ApiConfigIntegrityError(RuntimeError):
    """Raised when encrypted exchange credentials cannot be read safely."""


_grid_state_integrity_error = ""
_grid_history_integrity_error = ""
_api_config_integrity_error = ""
_api_config_read_error = ""
_api_config_write_error = ""
_api_config_tracked_path = ""
_api_config_file_was_present = False


def _set_grid_history_integrity_error(message: str):
    global _grid_history_integrity_error
    if message and _grid_history_integrity_error != message:
        logger.error(message)
    _grid_history_integrity_error = message


def _sync_api_config_integrity_error():
    global _api_config_integrity_error
    _api_config_integrity_error = " ".join(
        error for error in (_api_config_read_error, _api_config_write_error) if error
    )


def _set_api_config_read_error(message: str):
    global _api_config_read_error
    if message and _api_config_read_error != message:
        logger.error(message)
    _api_config_read_error = message
    _sync_api_config_integrity_error()


def _set_api_config_write_error(message: str):
    global _api_config_write_error
    if message and _api_config_write_error != message:
        logger.error(message)
    _api_config_write_error = message
    _sync_api_config_integrity_error()

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


def _fsync_parent_directory(path: str):
    if os.name == "nt":
        return
    directory = os.path.dirname(os.path.abspath(path)) or "."
    descriptor = None
    try:
        descriptor = os.open(
            directory,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        os.fsync(descriptor)
    except OSError:
        # The file itself is already durable. Some mounted filesystems do not
        # allow directory fsync, so treat this final metadata barrier as best effort.
        pass
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _cors_allowed_origins() -> list[str]:
    origins = os.getenv("CORS_ALLOWED_ORIGINS", "")
    return [item.strip() for item in origins.split(",") if item.strip()]


def _normalize_exchange(exchange: str | None) -> str:
    normalized = str(exchange or DEFAULT_EXCHANGE).strip().lower()
    return normalized if normalized in SUPPORTED_EXCHANGES else DEFAULT_EXCHANGE


def _exchange_prefix(exchange: str) -> str:
    return {"binance": "BINANCE", "aster": "ASTER"}.get(_normalize_exchange(exchange), "BYBIT")


def _default_api_config(exchange: str = DEFAULT_EXCHANGE) -> dict:
    return {
        "exchange": _normalize_exchange(exchange),
        "api_key": "",
        "api_secret": "",
        "testnet": False,
        "source": "none",
    }


def _env_config_for_exchange(exchange: str) -> dict | None:
    exchange = _normalize_exchange(exchange)
    prefix = _exchange_prefix(exchange)
    api_key = os.getenv(f"{prefix}_API_KEY", "").strip()
    api_secret = os.getenv(f"{prefix}_API_SECRET", "").strip()
    if exchange == "aster":
        api_key = api_key or os.getenv("ASTER_USER_ADDRESS", "").strip()
        api_secret = api_secret or os.getenv("ASTER_SIGNER_PRIVATE_KEY", "").strip()

    if not api_key or not api_secret:
        return None

    return {
        "exchange": exchange,
        "api_key": api_key,
        "api_secret": api_secret,
        "testnet": _parse_bool(os.getenv(f"{prefix}_TESTNET")),
        "source": "env",
    }


def _load_env_api_configs() -> dict[str, dict]:
    configs: dict[str, dict] = {}
    preferred = _normalize_exchange(os.getenv("GRID_EXCHANGE") or os.getenv("EXCHANGE"))
    ordered = [preferred] + sorted(SUPPORTED_EXCHANGES - {preferred})
    for exchange in ordered:
        config = _env_config_for_exchange(exchange)
        if config:
            configs[exchange] = config

    # Backward compatibility: old Binance deployments often only set BINANCE_*
    # without GRID_EXCHANGE. Pick it up even when Bybit is the default exchange.
    if "binance" not in configs:
        config = _env_config_for_exchange("binance")
        if config:
            configs["binance"] = config
    return configs


def _load_env_api_config() -> dict | None:
    configs = _load_env_api_configs()
    return _select_api_config(configs)


def _load_api_configs() -> dict[str, dict]:
    try:
        file_configs = _load_file_api_configs()
    except ApiConfigIntegrityError:
        file_configs = {}
    env_configs = _load_env_api_configs()
    merged = dict(env_configs)
    for exchange, config in file_configs.items():
        if config.get("api_key") and config.get("api_secret"):
            merged[exchange] = config
        elif exchange not in merged:
            merged[exchange] = config
    return merged


def _select_api_config(configs: dict[str, dict], exchange: str | None = None) -> dict | None:
    if not configs:
        return None
    if exchange:
        return configs.get(_normalize_exchange(exchange))
    preferred = _normalize_exchange(os.getenv("GRID_EXCHANGE") or os.getenv("EXCHANGE"))
    if preferred in configs:
        return configs[preferred]
    for candidate in ("binance", "aster", "bybit"):
        if candidate in configs:
            return configs[candidate]
    return next(iter(configs.values()))


def _load_api_config() -> dict:
    try:
        file_configs = _load_file_api_configs()
    except ApiConfigIntegrityError:
        file_configs = {}
    if file_configs:
        return _select_api_config(file_configs) or _default_api_config()
    env_configs = _load_env_api_configs()
    return _select_api_config(env_configs) or _default_api_config()


def _track_api_config_path() -> str:
    global _api_config_tracked_path, _api_config_file_was_present
    path = os.path.abspath(API_CONFIG_FILE)
    if path != _api_config_tracked_path:
        _api_config_tracked_path = path
        _api_config_file_was_present = os.path.exists(path)
    return path


def _mark_api_config_file_present():
    global _api_config_file_was_present
    _api_config_file_was_present = True


def _decode_api_config_entry(item: dict, exchange: str) -> tuple[dict, bool]:
    encrypted = item.get("encrypted", False)
    if not isinstance(encrypted, bool):
        raise ValueError("encrypted must be a boolean")

    api_key = item.get("api_key")
    api_secret = item.get("api_secret")
    if not isinstance(api_key, str) or not api_key:
        raise ValueError("api_key must be a non-empty string")
    if not isinstance(api_secret, str) or not api_secret:
        raise ValueError("api_secret must be a non-empty string")

    testnet = item.get("testnet", False)
    if not isinstance(testnet, bool):
        raise ValueError("testnet must be a boolean")

    if encrypted:
        api_key = decrypt_text(api_key)
        api_secret = decrypt_text(api_secret)
        if not api_key or not api_secret:
            raise ValueError("decrypted credentials must not be empty")

    return (
        {
            "api_key": api_key,
            "api_secret": api_secret,
            "exchange": exchange,
            "testnet": testnet,
            "source": "file",
        },
        not encrypted,
    )


def _load_file_api_configs() -> dict[str, dict]:
    with _config_file_lock:
        config_path = _track_api_config_path()
        if not os.path.exists(config_path):
            if _api_config_file_was_present:
                message = (
                    "The encrypted API configuration file disappeared after it was loaded. "
                    "Credential updates are blocked to prevent losing other exchanges."
                )
                _set_api_config_read_error(message)
                raise ApiConfigIntegrityError(message)
            _set_api_config_read_error("")
            return {}

        try:
            _mark_api_config_file_present()
            with open(config_path, "r", encoding="utf-8") as file:
                config = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            message = (
                "The encrypted API configuration file cannot be read safely. The original "
                "file is being preserved and credential updates are blocked."
            )
            _set_api_config_read_error(message)
            raise ApiConfigIntegrityError(message) from exc

        if not isinstance(config, dict):
            message = (
                "The encrypted API configuration file has an invalid root structure. The "
                "original file is being preserved and credential updates are blocked."
            )
            _set_api_config_read_error(message)
            raise ApiConfigIntegrityError(message)
        if not config:
            message = (
                "The encrypted API configuration file is unexpectedly empty. The original "
                "file is being preserved and credential updates are blocked."
            )
            _set_api_config_read_error(message)
            raise ApiConfigIntegrityError(message)

        try:
            if "configs" in config:
                if config.get("version") != 2:
                    raise ValueError("unsupported multi-exchange config version")
                raw_configs = config.get("configs")
                if not isinstance(raw_configs, dict) or not raw_configs:
                    raise ValueError("configs must be a non-empty object")
                loaded: dict[str, dict] = {}
                needs_migration = False
                for exchange_key, item in raw_configs.items():
                    if not isinstance(item, dict):
                        raise ValueError("each exchange config must be an object")
                    raw_exchange = str(exchange_key).strip().lower()
                    if raw_exchange not in SUPPORTED_EXCHANGES:
                        raise ValueError("unsupported exchange in API config")
                    declared_exchange = item.get("exchange", raw_exchange)
                    if not isinstance(declared_exchange, str):
                        raise ValueError("exchange must be a string")
                    if declared_exchange.strip().lower() != raw_exchange:
                        raise ValueError("exchange key and value do not match")
                    loaded_item, plaintext = _decode_api_config_entry(
                        item,
                        raw_exchange,
                    )
                    loaded[raw_exchange] = loaded_item
                    needs_migration = needs_migration or plaintext
                if needs_migration:
                    _write_api_configs(loaded)
                _set_api_config_read_error("")
                return loaded

            raw_exchange = str(config.get("exchange") or DEFAULT_EXCHANGE).strip().lower()
            if raw_exchange not in SUPPORTED_EXCHANGES:
                raise ValueError("unsupported exchange in API config")
            legacy, plaintext = _decode_api_config_entry(config, raw_exchange)
            if plaintext:
                # One-time migration for configs saved before encrypted storage existed.
                _write_api_configs({raw_exchange: legacy})
            _set_api_config_read_error("")
            return {raw_exchange: legacy}
        except ApiConfigIntegrityError:
            raise
        except Exception as exc:
            # Never log decrypted values. Preserve the source file so a bad key,
            # malformed entry, or failed migration cannot erase other exchanges.
            message = (
                "The encrypted API configuration cannot be decrypted or migrated safely. "
                "The original file is being preserved and credential updates are blocked."
            )
            _set_api_config_read_error(message)
            raise ApiConfigIntegrityError(message) from exc


def _write_api_configs(configs: dict[str, dict]):
    with _config_file_lock:
        config_path = _track_api_config_path()
        tmp_path = ""
        try:
            if not isinstance(configs, dict) or not configs:
                raise ValueError("at least one API configuration is required")

            backend = storage_backend()
            encrypted_configs = {}
            for exchange_key, config in configs.items():
                exchange = str(exchange_key).strip().lower()
                if exchange not in SUPPORTED_EXCHANGES:
                    raise ValueError("unsupported exchange in API config")
                if not isinstance(config, dict):
                    raise ValueError("each exchange config must be an object")
                declared_exchange = str(config.get("exchange") or exchange).strip().lower()
                if declared_exchange != exchange:
                    raise ValueError("exchange key and value do not match")
                api_key = config.get("api_key")
                api_secret = config.get("api_secret")
                if not isinstance(api_key, str) or not api_key:
                    raise ValueError("api_key must be a non-empty string")
                if not isinstance(api_secret, str) or not api_secret:
                    raise ValueError("api_secret must be a non-empty string")
                testnet = config.get("testnet", False)
                if not isinstance(testnet, bool):
                    raise ValueError("testnet must be a boolean")
                encrypted_configs[exchange] = {
                    "encrypted": True,
                    "backend": backend,
                    "exchange": exchange,
                    "api_key": encrypt_text(api_key),
                    "api_secret": encrypt_text(api_secret),
                    "testnet": testnet,
                }

            encrypted_config = {
                "version": 2,
                "encrypted": True,
                "backend": backend,
                "configs": encrypted_configs,
            }
            config_dir = os.path.dirname(config_path)
            os.makedirs(config_dir, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=config_dir,
                prefix=f".{os.path.basename(config_path)}.",
                suffix=".tmp",
                delete=False,
            ) as file:
                tmp_path = file.name
                if os.name != "nt":
                    os.chmod(tmp_path, 0o600)
                json.dump(encrypted_config, file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_path, config_path)
            tmp_path = ""
            _fsync_parent_directory(config_path)
            _private_chmod(config_path)
        except Exception:
            _set_api_config_write_error(
                "The encrypted API configuration file cannot be written safely. The "
                "previous credential file is being preserved."
            )
            raise
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except FileNotFoundError:
                    pass
                except OSError:
                    logger.warning(
                        "Failed to remove temporary API config file path=%s",
                        tmp_path,
                    )
        _mark_api_config_file_present()
        _set_api_config_read_error("")
        _set_api_config_write_error("")


def _save_api_config(config: dict):
    with _config_file_lock:
        exchange = _normalize_exchange(config.get("exchange"))
        configs = _load_file_api_configs()
        configs[exchange] = {
            "exchange": exchange,
            "api_key": str(config.get("api_key", "")),
            "api_secret": str(config.get("api_secret", "")),
            "testnet": bool(config.get("testnet", False)),
            "source": "file",
        }
        _write_api_configs(configs)


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
    if exchange == "aster":
        return AsterFuturesClient(api_key, api_secret, testnet)
    return BybitClient(api_key, api_secret, testnet)


def _engine_key(exchange: str, symbol: str) -> str:
    return f"{_normalize_exchange(exchange)}:{str(symbol or '').upper().strip()}"


def _engine_exchange(engine: GridEngine | None) -> str:
    if not engine:
        return _active_exchange
    return _normalize_exchange(engine.config.get("exchange") or getattr(engine.client, "exchange", None))


def _engine_symbol(engine: GridEngine | None) -> str:
    if not engine:
        return ""
    return str(engine.config.get("symbol", "")).upper().strip()


def _engine_snapshot() -> list[GridEngine]:
    with _engines_lock:
        return list(_engines.values())


def _engine_requires_cleanup(engine: GridEngine | None) -> bool:
    if not engine:
        return False
    return bool(
        engine.running
        or engine.active_orders
        or engine.opening_order
        or engine.pending_reduce_action
        or engine.paused_replacements
        or engine.initialization_in_progress
        or engine.risk_shutdown_pending
        or engine.manual_stop_pending
        or engine.initialization_failed
        or engine._qty_reaches_accounting_step(engine._grid_position_qty())
    )


def _get_engine(exchange: str | None, symbol: str) -> GridEngine | None:
    symbol = str(symbol or "").upper().strip()
    with _engines_lock:
        if exchange:
            return _engines.get(_engine_key(exchange, symbol))
        matches = [engine for engine in _engines.values() if _engine_symbol(engine) == symbol]
        if len(matches) == 1:
            return matches[0]
        return _engines.get(symbol)


def _configured_exchange(config: dict | None) -> bool:
    return bool(config and config.get("api_key") and config.get("api_secret"))


def _refresh_active_exchange(exchange: str | None = None):
    global _active_exchange, _api_config, _client
    if exchange:
        _active_exchange = _normalize_exchange(exchange)
    elif _active_exchange not in _api_configs:
        selected = _select_api_config(_api_configs)
        _active_exchange = _normalize_exchange(selected.get("exchange") if selected else DEFAULT_EXCHANGE)
    _api_config = _api_configs.get(_active_exchange) or _default_api_config(_active_exchange)
    _client = _clients.get(_active_exchange)


def _client_for_exchange(exchange: str | None = None, *, require_config: bool = True):
    exchange = _normalize_exchange(exchange or _active_exchange)
    # Compatibility for tests and legacy scripts that monkeypatch main._client.
    if exchange == _active_exchange and _client and _client is not _clients.get(exchange):
        return _client

    client = _clients.get(exchange)
    if client:
        return client

    config = _api_configs.get(exchange)
    if _configured_exchange(config):
        client = _build_client_from_config(config)
        _clients[exchange] = client
        return client

    if exchange == _normalize_exchange(_api_config.get("exchange")) and _client:
        return _client

    if require_config:
        raise HTTPException(status_code=400, detail=f"Please configure {exchange.title()} API first")
    return _build_client(exchange, "", "", bool((config or {}).get("testnet", False)))


_api_configs = _load_api_configs()
_api_config = _select_api_config(_api_configs) or _default_api_config()
_active_exchange = _normalize_exchange(_api_config.get("exchange"))
_clients = {
    exchange: _build_client_from_config(config)
    for exchange, config in _api_configs.items()
    if config.get("api_key") and config.get("api_secret")
}
_client = _clients.get(_active_exchange)


def _load_grid_state_file() -> dict:
    global _grid_state_integrity_error
    with _state_files_lock:
        if not os.path.exists(GRID_STATE_FILE):
            _grid_state_integrity_error = ""
            return {"version": 1, "grids": {}}

        try:
            with open(GRID_STATE_FILE, "r", encoding="utf-8") as file:
                state = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            message = (
                "The durable grid state file cannot be read safely. New grid starts are "
                "blocked so existing exchange work cannot be mistaken for an empty account."
            )
            _grid_state_integrity_error = message
            raise GridStateIntegrityError(message) from exc

        if not isinstance(state, dict):
            message = (
                "The durable grid state file has an invalid root structure. New grid starts "
                "are blocked until the ledger is reviewed."
            )
            _grid_state_integrity_error = message
            raise GridStateIntegrityError(message)
        grids = state.get("grids")
        if grids is not None and not isinstance(grids, dict):
            message = (
                "The durable grid state file has an invalid grids structure. New grid starts "
                "are blocked until the ledger is reviewed."
            )
            _grid_state_integrity_error = message
            raise GridStateIntegrityError(message)
        state.setdefault("version", 1)
        state.setdefault("grids", {})
        _grid_state_integrity_error = ""
        return state


def _write_grid_state_file(state: dict):
    global _grid_state_integrity_error
    with _state_files_lock:
        state_dir = os.path.dirname(GRID_STATE_FILE)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
        tmp_path = f"{GRID_STATE_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp_path, GRID_STATE_FILE)
        _fsync_parent_directory(GRID_STATE_FILE)
        _private_chmod(GRID_STATE_FILE)
        _grid_state_integrity_error = ""


def _assert_grid_state_integrity():
    try:
        _load_grid_state_file()
    except GridStateIntegrityError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _load_grid_history_file() -> dict:
    with _state_files_lock:
        if not os.path.exists(GRID_HISTORY_FILE):
            _set_grid_history_integrity_error("")
            return {"version": 1, "runs": []}

        try:
            with open(GRID_HISTORY_FILE, "r", encoding="utf-8") as file:
                history = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            message = (
                "The grid history file cannot be read safely. The original file is being "
                "preserved and history updates are paused until it is repaired."
            )
            _set_grid_history_integrity_error(message)
            raise GridHistoryIntegrityError(message) from exc

        if not isinstance(history, dict):
            message = (
                "The grid history file has an invalid root structure. The original file is "
                "being preserved and history updates are paused until it is repaired."
            )
            _set_grid_history_integrity_error(message)
            raise GridHistoryIntegrityError(message)
        runs = history.get("runs")
        if runs is not None and not isinstance(runs, list):
            message = (
                "The grid history file has an invalid runs structure. The original file is "
                "being preserved and history updates are paused until it is repaired."
            )
            _set_grid_history_integrity_error(message)
            raise GridHistoryIntegrityError(message)
        history.setdefault("version", 1)
        history.setdefault("runs", [])
        _set_grid_history_integrity_error("")
        return history


def _write_grid_history_file(history: dict):
    with _state_files_lock:
        history_dir = os.path.dirname(GRID_HISTORY_FILE)
        if history_dir:
            os.makedirs(history_dir, exist_ok=True)
        tmp_path = f"{GRID_HISTORY_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(history, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp_path, GRID_HISTORY_FILE)
        _fsync_parent_directory(GRID_HISTORY_FILE)
        _private_chmod(GRID_HISTORY_FILE)
        _set_grid_history_integrity_error("")


def _history_record_from_engine(engine: GridEngine, status: str = "running") -> dict:
    config = engine.config
    return {
        "run_id": config.get("run_id", ""),
        "symbol": str(config.get("symbol", "")).upper(),
        "exchange": _normalize_exchange(config.get("exchange") or _engine_exchange(engine)),
        "direction": config.get("direction", ""),
        "grid_mode": config.get("grid_mode", "arithmetic"),
        "lower_price": config.get("lower_price"),
        "upper_price": config.get("upper_price"),
        "grid_count": config.get("grid_count"),
        "leverage": config.get("leverage"),
        "total_investment": config.get("total_investment"),
        "position_sizing_mode": config.get("position_sizing_mode", "investment"),
        "grid_order_qty": config.get("grid_order_qty"),
        "initial_order_type": config.get("initial_order_type", "market"),
        "initial_order_price": config.get("initial_order_price"),
        "maker_fee_rate": engine._maker_fee_rate(),
        "taker_fee_rate": engine._taker_fee_rate(),
        "fee_rate_source": config.get("fee_rate_source", "saved_config"),
        "fee_rate_fetched_at": config.get("fee_rate_fetched_at"),
        "status": status,
        "started_at": engine.start_time or time.time(),
        "updated_at": time.time(),
        "stopped_at": time.time() if status in {"stopped", "closed"} else None,
        "initial_side": engine.initial_side,
        "initial_qty": round(engine.initial_qty, 8),
        "baseline_position_side": engine.baseline_position_side,
        "baseline_position_qty": round(engine.baseline_position_qty, 8),
        "completed_pairs": engine.completed_pairs,
        "gross_profit": round(engine.gross_profit, 4),
        "net_profit": round(engine.total_profit, 4),
        "total_fee": round(engine.total_fee, 4),
        "total_volume": round(engine.total_volume, 4),
        "filled_count": len(engine.filled_orders),
        "last_message": engine.trigger_message,
    }


def _round_down_steps(value: float, step: str) -> int:
    from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

    value_decimal = Decimal(str(value))
    step_decimal = Decimal(str(step))
    nearest_steps = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_HALF_UP)
    nearest_value = nearest_steps * step_decimal
    tolerance = max(abs(step_decimal) * Decimal("1e-9"), Decimal("1e-18"))
    if abs(value_decimal - nearest_value) <= tolerance:
        value_decimal = nearest_value
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


def _exchange_fee_rates(client, symbol: str) -> dict:
    try:
        response = client.get_fee_rates(symbol)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch account fee rates for {symbol}: {exc}",
        ) from exc
    if not isinstance(response, dict):
        raise HTTPException(status_code=502, detail="Exchange returned an invalid fee rate response")
    if response.get("retCode") != 0:
        raise HTTPException(
            status_code=502,
            detail=response.get("retMsg", f"Failed to fetch account fee rates for {symbol}"),
        )

    result = response.get("result") or {}
    if not isinstance(result, dict):
        raise HTTPException(status_code=502, detail="Exchange returned an invalid fee rate response")
    try:
        maker_fee_rate = normalize_fee_rate(result.get("makerFeeRate"), "maker fee rate")
        taker_fee_rate = normalize_fee_rate(result.get("takerFeeRate"), "taker fee rate")
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    try:
        fetched_at = int(result.get("fetchedAt") or int(time.time() * 1000))
    except (TypeError, ValueError):
        fetched_at = int(time.time() * 1000)
    return {
        "symbol": symbol,
        "maker_fee_rate": maker_fee_rate,
        "taker_fee_rate": taker_fee_rate,
        "source": str(result.get("source") or "exchange"),
        "fetched_at": fetched_at,
    }


def _with_exchange_fee_rates(client, cfg: "GridConfig", symbol: str) -> tuple["GridConfig", dict]:
    rates = _exchange_fee_rates(client, symbol)
    updated = cfg.model_copy(
        update={
            "fee_rate": float(rates["taker_fee_rate"]),
            "maker_fee_rate": float(rates["maker_fee_rate"]),
            "taker_fee_rate": float(rates["taker_fee_rate"]),
        }
    )
    _validate_fee_rates(updated)
    return updated, rates


def _position_sizing_mode(cfg: "GridConfig") -> str:
    return str(cfg.position_sizing_mode or "investment").lower().strip()


def _preview_grid(client, cfg, symbol: str, direction: str, grid_mode: str) -> dict:
    ticker = client.get_ticker(symbol)
    if ticker.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=ticker.get("retMsg", "Failed to fetch current price"))
    current_price = float(ticker["result"]["list"][0]["lastPrice"])
    initial_order_type = cfg.initial_order_type.lower().strip()
    sizing_mode = _position_sizing_mode(cfg)

    info_resp = client.get_instrument_info(symbol)
    if info_resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=info_resp.get("retMsg", "Failed to fetch instrument info"))
    instrument = info_resp["result"]["list"][0]
    tick_size_text = str(instrument["priceFilter"]["tickSize"])
    tick_size = float(tick_size_text)
    lot_filter = instrument["lotSizeFilter"]
    explicit_market_filter = instrument.get("marketLotSizeFilter")
    market_filter = explicit_market_filter or lot_filter
    qty_step = lot_filter["qtyStep"]
    min_qty = float(lot_filter["minOrderQty"])
    market_qty_step = str(market_filter.get("qtyStep") or qty_step)
    market_min_qty = float(market_filter.get("minOrderQty") or min_qty)
    if explicit_market_filter:
        raw_max_market_qty = explicit_market_filter.get("maxOrderQty")
    else:
        raw_max_market_qty = (
            lot_filter.get("maxMktOrderQty")
            or lot_filter.get("maxMarketOrderQty")
            or lot_filter.get("maxOrderQty")
        )
    max_market_qty = float(raw_max_market_qty or 0)

    reference_price = current_price
    if direction in {"long", "short"} and initial_order_type in {"limit", "post_only"}:
        open_side = "Buy" if direction == "long" else "Sell"
        maker_safe_price = current_price - tick_size if open_side == "Buy" else current_price + tick_size
        if cfg.initial_order_price is not None:
            configured_price = float(cfg.initial_order_price)
            if initial_order_type == "limit":
                reference_price = configured_price
            elif open_side == "Buy" and configured_price < current_price:
                reference_price = configured_price
            elif open_side == "Sell" and configured_price > current_price:
                reference_price = configured_price
            else:
                reference_price = maker_safe_price
        else:
            reference_price = maker_safe_price

    levels = _calculate_grid_levels(cfg.lower_price, cfg.upper_price, cfg.grid_count, grid_mode)
    rounded_levels = [float(client.round_to_step(level, tick_size_text)) for level in levels]
    if any(
        current <= previous
        for previous, current in pairwise(rounded_levels)
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Configured grid levels collapse to duplicate exchange prices; "
                "reduce the grid count or widen the price range"
            ),
        )
    if not (levels[0] < reference_price < levels[-1]):
        raise HTTPException(
            status_code=400,
            detail=f"Reference price {reference_price} must stay inside the configured range {levels[0]} - {levels[-1]}",
        )

    if direction == "long":
        active_targets = [
            (idx, levels[idx + 1], "Sell")
            for idx in range(len(levels) - 1)
            if levels[idx + 1] > reference_price
        ]
    elif direction == "short":
        active_targets = [
            (idx, levels[idx], "Buy")
            for idx in range(len(levels) - 1)
            if levels[idx] < reference_price
        ]
    else:
        active_targets = [
            (idx, levels[idx], "Buy")
            for idx in range(len(levels) - 1)
            if levels[idx] < reference_price
        ] + [
            (idx, levels[idx + 1], "Sell")
            for idx in range(len(levels) - 1)
            if levels[idx + 1] > reference_price
        ]

    active_count = len(active_targets)
    if active_count <= 0:
        raise HTTPException(status_code=400, detail="No valid grid targets were found around current price")

    if sizing_mode == "fixed_grid_qty":
        per_grid_steps = _round_down_steps(float(cfg.grid_order_qty or 0), qty_step)
        if per_grid_steps <= 0:
            raise HTTPException(status_code=400, detail="grid_order_qty is too small for this symbol")
        total_steps = per_grid_steps * active_count
        per_order_qtys = [_steps_to_qty(per_grid_steps, qty_step) for _ in range(active_count)]
    else:
        raw_total_qty = (cfg.total_investment * cfg.leverage) / reference_price
        total_steps = _round_down_steps(raw_total_qty, qty_step)
        base_steps = total_steps // active_count
        remainder_steps = total_steps % active_count
        per_order_qtys = [
            _steps_to_qty(base_steps + (1 if index < remainder_steps else 0), qty_step)
            for index in range(active_count)
        ]
    if total_steps < active_count:
        raise HTTPException(status_code=400, detail="Total investment is too small for this symbol and grid count")

    total_qty = _steps_to_qty(total_steps, qty_step)

    if direction in {"long", "short"} and initial_order_type == "market":
        market_steps = _round_down_steps(total_qty, market_qty_step)
        market_total_qty = _steps_to_qty(market_steps, market_qty_step)
        if market_total_qty < market_min_qty:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Initial market quantity {market_total_qty} is below the exchange minimum "
                    f"{market_min_qty}"
                ),
            )
        if max_market_qty > 0 and market_total_qty > max_market_qty:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Initial market quantity {market_total_qty} exceeds the exchange single-order "
                    f"maximum {max_market_qty}; reduce the per-grid quantity or use a limit opening order"
                ),
            )
        if sizing_mode == "fixed_grid_qty":
            tolerance = max(float(market_qty_step) * 1e-9, 1e-18)
            if abs(market_total_qty - total_qty) > tolerance:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "The requested fixed per-grid quantity cannot be represented exactly by the "
                        "exchange market-order quantity step; use a compatible quantity or a limit opening order"
                    ),
                )

        # A market fill must be fully representable by subsequent limit-grid orders.
        coverable_steps = _round_down_steps(market_total_qty, qty_step)
        coverable_qty = _steps_to_qty(coverable_steps, qty_step)
        tolerance = max(float(qty_step) * 1e-9, 1e-18)
        if abs(coverable_qty - market_total_qty) > tolerance:
            raise HTTPException(
                status_code=400,
                detail=(
                    "The exchange market quantity step is incompatible with its limit-order step; "
                    "use a limit opening order so every filled unit can be protected"
                ),
            )
        total_steps = coverable_steps
        total_qty = coverable_qty
        base_steps = total_steps // active_count
        remainder_steps = total_steps % active_count
        per_order_qtys = [
            _steps_to_qty(base_steps + (1 if index < remainder_steps else 0), qty_step)
            for index in range(active_count)
        ]

    limit_qty_tolerance = max(float(qty_step) * 1e-9, 1e-18)
    if min(per_order_qtys) + limit_qty_tolerance < min_qty:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Per-grid quantity {min(per_order_qtys)} is below the exchange limit-order "
                f"minimum {min_qty}; increase the per-grid quantity or investment"
            ),
        )

    if grid_mode == "geometric":
        ratio = (levels[1] / levels[0]) if len(levels) > 1 else 1
        grid_profit_pct = (ratio - 1) * 100
        grid_step = reference_price * (ratio - 1)
    else:
        grid_step = levels[1] - levels[0] if len(levels) > 1 else 0
        grid_profit_pct = (grid_step / reference_price) * 100 if reference_price > 0 else 0

    average_qty = total_qty / active_count
    maker_fee_rate = max(0.0, float(cfg.maker_fee_rate if cfg.maker_fee_rate is not None else cfg.fee_rate or 0))
    taker_fee_rate = max(0.0, float(cfg.taker_fee_rate if cfg.taker_fee_rate is not None else cfg.fee_rate or 0))
    opening_fee_rate = maker_fee_rate if initial_order_type == "post_only" else taker_fee_rate
    grid_fee_rate = maker_fee_rate
    gross_profit = grid_step * average_qty
    open_fee = average_qty * reference_price * opening_fee_rate
    close_fee = average_qty * reference_price * grid_fee_rate
    fee = open_fee + close_fee

    return {
        "symbol": symbol,
        "current_price": current_price,
        "reference_price": reference_price,
        "position_sizing_mode": sizing_mode,
        "grid_order_qty": cfg.grid_order_qty,
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
        "market_qty_step": market_qty_step,
        "market_min_qty": market_min_qty,
        "max_market_qty": max_market_qty,
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
        return False

    with _state_files_lock:
        try:
            history = _load_grid_history_file()
        except GridHistoryIntegrityError:
            return False
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

        history["runs"] = sorted(
            runs,
            key=lambda item: float(item.get("started_at") or 0),
            reverse=True,
        )[:500]
        history["updated_at"] = time.time()
        try:
            _write_grid_history_file(history)
        except (OSError, TypeError, ValueError) as exc:
            _set_grid_history_integrity_error(
                "The grid history file cannot be written safely. Trading state remains "
                "enabled, but history updates are paused until the file path is repaired."
            )
            logger.debug("Grid history write failure", exc_info=exc)
            return False
        return True


def _save_engine_state(engine: GridEngine):
    symbol = str(engine.config.get("symbol", "")).upper()
    if not symbol:
        return

    exchange = _engine_exchange(engine)
    engine.config["exchange"] = exchange
    history_status = "running" if engine.running else "saved"
    with _state_files_lock:
        state = _load_grid_state_file()
        state["updated_at"] = time.time()
        state.setdefault("grids", {})[_engine_key(exchange, symbol)] = engine.to_state()
        _write_grid_state_file(state)

        now = time.monotonic()
        previous_status = getattr(engine, "_last_history_state_status", "")
        try:
            previous_write = float(
                getattr(engine, "_last_history_state_write_at", 0.0) or 0.0
            )
        except (TypeError, ValueError):
            previous_write = 0.0
        if (
            history_status != previous_status
            or now - previous_write >= HISTORY_STATE_UPDATE_INTERVAL_SECONDS
        ):
            _upsert_grid_history(engine, history_status)
            engine._last_history_state_status = history_status
            engine._last_history_state_write_at = now


def _delete_engine_state(symbol: str, exchange: str | None = None):
    with _state_files_lock:
        state = _load_grid_state_file()
        grids = state.setdefault("grids", {})
        symbol = symbol.upper()
        if exchange:
            grids.pop(_engine_key(exchange, symbol), None)
        else:
            for key in (symbol, *[item for item in list(grids) if item.endswith(f":{symbol}")]):
                grids.pop(key, None)
        state["updated_at"] = time.time()
        _write_grid_state_file(state)


def _restore_saved_engines():
    try:
        state = _load_grid_state_file()
    except GridStateIntegrityError as exc:
        logger.critical("Grid state restore blocked by ledger integrity failure: %s", exc)
        return
    legacy_exchange = _normalize_exchange(state.get("exchange"))

    for state_key, engine_state in list(state.get("grids", {}).items()):
        config = dict(engine_state.get("config") or {})
        if not config:
            continue
        symbol = str(config.get("symbol") or str(state_key).split(":")[-1]).upper()
        exchange = _normalize_exchange(config.get("exchange") or engine_state.get("exchange") or legacy_exchange)
        key = _engine_key(exchange, symbol)
        with _engines_lock:
            if key in _engines:
                continue
        if not _configured_exchange(_api_configs.get(exchange)):
            logger.warning("Saved grid skipped because API is not configured exchange=%s symbol=%s", exchange, symbol)
            continue

        config["symbol"] = symbol
        config["exchange"] = exchange
        engine = GridEngine(_client_for_exchange(exchange), config, state_callback=_save_engine_state)
        try:
            engine.restore_state(engine_state)
            engine.config["exchange"] = exchange
            with _engines_lock:
                _engines[key] = engine
            cleanup_must_resume = bool(
                engine.manual_stop_pending
                or engine.risk_shutdown_pending
                or engine.pending_reduce_action
            )
            strategy_can_resume = bool(
                engine.grid_ready
                or engine.waiting_trigger
                or engine.waiting_initial_order
            )
            can_restart = not engine._stopping and (
                cleanup_must_resume
                or strategy_can_resume
            )
            if can_restart:
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    logger.warning("Saved grid restored without running event loop symbol=%s", config["symbol"])
                    can_restart = False
            if can_restart:
                engine.start()
            else:
                engine.running = False
                engine._persist_state()
            if state_key != key:
                with _state_files_lock:
                    migrated_state = _load_grid_state_file()
                    grids = migrated_state.setdefault("grids", {})
                    if key in grids and state_key in grids:
                        grids.pop(state_key, None)
                        migrated_state["updated_at"] = time.time()
                        _write_grid_state_file(migrated_state)
        except Exception as exc:
            # Keep the saved state on disk so the UI/risk checks can still show the problem.
            logger.exception("Failed to restore saved grid symbol=%s: %s", config.get("symbol"), exc)
            with _engines_lock:
                _engines.pop(key, None)


@asynccontextmanager
async def lifespan(_: FastAPI):
    _restore_saved_engines()
    yield
    for engine in _engine_snapshot():
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
    exchange: str | None = None
    symbol: str
    direction: str
    upper_price: float
    lower_price: float
    grid_count: int
    total_investment: float = 0
    leverage: int = 1
    position_sizing_mode: str = "investment"
    grid_order_qty: float | None = None
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
    try:
        _load_file_api_configs()
    except ApiConfigIntegrityError:
        # Return the in-memory status together with the durable storage warning.
        pass
    _refresh_active_exchange()
    api_key = _api_config.get("api_key", "")
    configs = {}
    for exchange in sorted(SUPPORTED_EXCHANGES):
        config = _api_configs.get(exchange) or _default_api_config(exchange)
        configs[exchange] = {
            "exchange": exchange,
            "api_key": _mask_api_key(config.get("api_key", "")),
            "testnet": bool(config.get("testnet", False)),
            "configured": bool(config.get("api_key")),
            "source": config.get("source", "none"),
        }
    return {
        "exchange": _normalize_exchange(_api_config.get("exchange")),
        "active_exchange": _active_exchange,
        "exchanges": sorted(SUPPORTED_EXCHANGES),
        "configs": configs,
        "api_key": _mask_api_key(api_key),
        "testnet": _api_config.get("testnet", False),
        "configured": bool(api_key),
        "source": _api_config.get("source", "none"),
        "storage": storage_backend(),
        "storage_error": _api_config_integrity_error,
    }


@app.post("/api/config")
def set_config(cfg: ApiConfig):
    global _api_configs, _clients

    exchange = _normalize_exchange(cfg.exchange)
    if any(engine.running and _engine_exchange(engine) == exchange for engine in _engine_snapshot()):
        raise HTTPException(status_code=400, detail=f"Stop running {exchange.title()} grids before changing this API config")

    try:
        _load_file_api_configs()
    except ApiConfigIntegrityError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

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
    except ApiConfigIntegrityError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except (OSError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail=f"API verified but not saved securely: {exc}") from exc

    _api_configs[exchange] = {**saved_config, "source": "file"}
    _clients[exchange] = candidate
    _refresh_active_exchange(exchange)
    return {"ok": True, "message": f"{exchange.title()} API config saved"}


@app.get("/api/price/{symbol}")
def get_price(symbol: str, exchange: str | None = None):
    exchange = _normalize_exchange(exchange or _active_exchange)
    client = _client_for_exchange(exchange, require_config=False)
    resp = client.get_ticker(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch ticker"))

    data = resp["result"]["list"][0]
    return {
        "symbol": data["symbol"],
        "exchange": exchange,
        "last_price": data["lastPrice"],
        "index_price": data.get("indexPrice", ""),
        "mark_price": data.get("markPrice", ""),
        "price_24h_pcnt": data.get("price24hPcnt", "0"),
        "volume_24h": data.get("volume24h", "0"),
    }


@app.get("/api/balance")
def get_balance(exchange: str | None = None):
    client = _get_client(exchange)
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


@app.get("/api/fees/{symbol}")
def get_fee_rates(symbol: str, exchange: str | None = None):
    exchange = _normalize_exchange(exchange or _active_exchange)
    client = _get_client(exchange)
    rates = _exchange_fee_rates(client, symbol.upper().strip())
    return {"exchange": exchange, **rates}


@app.get("/api/positions/{symbol}")
def get_positions(symbol: str, exchange: str | None = None):
    client = _get_client(exchange)
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
    exchange = _normalize_exchange(cfg.exchange or _active_exchange)
    client = _get_client(exchange)
    symbol = cfg.symbol.upper().strip()
    direction = cfg.direction.lower().strip()
    grid_mode = cfg.grid_mode.lower().strip()
    sizing_mode = _position_sizing_mode(cfg)

    if cfg.upper_price <= cfg.lower_price:
        raise HTTPException(status_code=400, detail="upper_price must be greater than lower_price")
    if cfg.grid_count < 2 or cfg.grid_count > 100:
        raise HTTPException(status_code=400, detail="grid_count must be between 2 and 100")
    if sizing_mode not in {"investment", "fixed_grid_qty"}:
        raise HTTPException(status_code=400, detail="position_sizing_mode must be investment or fixed_grid_qty")
    if sizing_mode == "investment" and cfg.total_investment <= 0:
        raise HTTPException(status_code=400, detail="total_investment must be greater than 0")
    if sizing_mode == "fixed_grid_qty" and (cfg.grid_order_qty is None or cfg.grid_order_qty <= 0):
        raise HTTPException(status_code=400, detail="grid_order_qty must be greater than 0")
    if cfg.leverage < 1 or cfg.leverage > 125:
        raise HTTPException(status_code=400, detail="leverage must be between 1 and 125")
    if direction not in {"long", "short", "neutral"}:
        raise HTTPException(status_code=400, detail="direction must be long, short, or neutral")
    if grid_mode not in {"arithmetic", "geometric"}:
        raise HTTPException(status_code=400, detail="grid_mode must be arithmetic or geometric")
    if cfg.initial_order_type.lower().strip() not in {"market", "limit", "post_only"}:
        raise HTTPException(status_code=400, detail="initial_order_type must be market, limit, or post_only")
    if direction == "neutral" and cfg.initial_order_type.lower().strip() != "market":
        raise HTTPException(status_code=400, detail="limit initial orders are only supported for long or short grids")
    if cfg.initial_order_price is not None and cfg.initial_order_price <= 0:
        raise HTTPException(status_code=400, detail="initial_order_price must be greater than 0")

    cfg, fee_rates = _with_exchange_fee_rates(client, cfg, symbol)

    preview = _preview_grid(client, cfg, symbol, direction, grid_mode)
    preview["exchange"] = exchange
    preview["fee_rate_source"] = fee_rates["source"]
    preview["fee_rate_fetched_at"] = fee_rates["fetched_at"]
    return preview


@app.post("/api/grid/start")
async def start_grid(cfg: GridConfig):
    exchange = _normalize_exchange(cfg.exchange or _active_exchange)
    client = _get_client(exchange)
    symbol = cfg.symbol.upper().strip()
    direction = cfg.direction.lower().strip()
    grid_mode = cfg.grid_mode.lower().strip()
    initial_order_type = cfg.initial_order_type.lower().strip()
    sizing_mode = _position_sizing_mode(cfg)
    engine_key = _engine_key(exchange, symbol)
    if cfg.upper_price <= cfg.lower_price:
        raise HTTPException(status_code=400, detail="upper_price must be greater than lower_price")
    if cfg.grid_count < 2 or cfg.grid_count > 100:
        raise HTTPException(status_code=400, detail="grid_count must be between 2 and 100")
    if sizing_mode not in {"investment", "fixed_grid_qty"}:
        raise HTTPException(status_code=400, detail="position_sizing_mode must be investment or fixed_grid_qty")
    if sizing_mode == "investment" and cfg.total_investment <= 0:
        raise HTTPException(status_code=400, detail="total_investment must be greater than 0")
    if sizing_mode == "fixed_grid_qty" and (cfg.grid_order_qty is None or cfg.grid_order_qty <= 0):
        raise HTTPException(status_code=400, detail="grid_order_qty must be greater than 0")
    if cfg.leverage < 1 or cfg.leverage > 125:
        raise HTTPException(status_code=400, detail="leverage must be between 1 and 125")
    if direction not in {"long", "short", "neutral"}:
        raise HTTPException(status_code=400, detail="direction must be long, short, or neutral")
    if grid_mode not in {"arithmetic", "geometric"}:
        raise HTTPException(status_code=400, detail="grid_mode must be arithmetic or geometric")
    if initial_order_type not in {"market", "limit", "post_only"}:
        raise HTTPException(status_code=400, detail="initial_order_type must be market, limit, or post_only")
    if direction == "neutral" and initial_order_type != "market":
        raise HTTPException(status_code=400, detail="limit initial orders are only supported for long or short grids")
    if cfg.initial_order_price is not None and cfg.initial_order_price <= 0:
        raise HTTPException(status_code=400, detail="initial_order_price must be greater than 0")

    cfg, fee_rates = _with_exchange_fee_rates(client, cfg, symbol)

    # Never interpret a damaged durable ledger as an empty account. This check
    # runs before any exchange order or position-changing request.
    _assert_grid_state_integrity()

    engine_config = cfg.model_dump()
    engine_config["exchange"] = exchange
    engine_config["symbol"] = symbol
    engine_config["direction"] = direction
    engine_config["grid_mode"] = grid_mode
    engine_config["initial_order_type"] = initial_order_type
    engine_config["position_sizing_mode"] = sizing_mode
    engine_config["fee_rate_source"] = fee_rates["source"]
    engine_config["fee_rate_fetched_at"] = fee_rates["fetched_at"]
    engine_config["run_id"] = f"{symbol}_{int(time.time())}_{os.urandom(3).hex()}"
    engine_config["strict_order_ownership"] = True
    engine = GridEngine(client, engine_config, state_callback=_save_engine_state)

    with _engines_lock:
        existing_engine = _engines.get(engine_key)
        if engine_key in _starting_engine_keys or _engine_requires_cleanup(existing_engine):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"An existing grid for {symbol} still has running, initializing, or "
                    "unconfirmed exchange work; stop and reconcile it before starting a new grid"
                ),
            )
        _starting_engine_keys.add(engine_key)

    try:
        open_response = client.get_open_orders(symbol)
        if open_response.get("retCode") != 0:
            raise RuntimeError(
                open_response.get("retMsg", "Failed to verify existing exchange orders")
            )
        existing_grid_orders = [
            item
            for item in open_response.get("result", {}).get("list", [])
            if _is_grid_order(item)
        ]
        if existing_grid_orders:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Found {len(existing_grid_orders)} existing grid-tagged exchange order(s) "
                    f"for {symbol}; review or cancel those orders before starting a new run"
                ),
            )
        await engine.initialize()
        with _engines_lock:
            _engines[engine_key] = engine
        engine.start()
        _save_engine_state(engine)
    except HTTPException:
        raise
    except Exception as exc:
        has_exchange_state = bool(
            engine.active_orders
            or engine.opening_order
            or engine.pending_reduce_action
            or engine._qty_reaches_accounting_step(engine._grid_position_qty())
        )
        if has_exchange_state:
            engine.initialization_in_progress = False
            engine.initialization_failed = True
            engine.manual_stop_pending = True
            engine.grid_ready = False
            engine.trigger_message = (
                f"Grid initialization failed after exchange work may have started: {exc}. "
                "Managed orders are being cancelled; review the retained position before retrying."
            )
            with _engines_lock:
                _engines[engine_key] = engine
            try:
                if not engine.running:
                    engine.start()
                else:
                    engine._persist_state()
            except Exception as recovery_exc:
                logger.exception(
                    "Failed to start initialization recovery exchange=%s symbol=%s: %s",
                    exchange,
                    symbol,
                    recovery_exc,
                )
            raise HTTPException(status_code=409, detail=engine.trigger_message) from exc

        engine.running = False
        if engine._task:
            engine._task.cancel()
        with _engines_lock:
            if _engines.get(engine_key) is engine:
                _engines.pop(engine_key, None)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        with _engines_lock:
            _starting_engine_keys.discard(engine_key)

    return {"ok": True, "message": f"{exchange.title()} {symbol} {direction} grid started"}


@app.post("/api/grid/stop")
async def stop_grid():
    pending = [engine for engine in _engine_snapshot() if _engine_requires_cleanup(engine)]
    if not pending:
        raise HTTPException(status_code=400, detail="No active grid")
    if len(pending) > 1:
        raise HTTPException(status_code=400, detail="Multiple grids are running; stop by symbol")

    engine = pending[0]
    return await _stop_grid(_engine_symbol(engine), _engine_exchange(engine))


@app.post("/api/grid/stop/{symbol}")
async def stop_grid_symbol(symbol: str, exchange: str | None = None):
    return await _stop_grid(symbol.upper().strip(), exchange)


@app.post("/api/grid/stop-all")
async def stop_all_grids():
    pending = [engine for engine in _engine_snapshot() if _engine_requires_cleanup(engine)]
    if not pending:
        raise HTTPException(status_code=400, detail="No active grid")

    for engine in pending:
        symbol = str(engine.config.get("symbol", "")).upper()
        exchange = _engine_exchange(engine)
        await engine.stop()
        _upsert_grid_history(engine, "stopped")
        if symbol:
            _delete_engine_state(symbol, exchange)
            with _engines_lock:
                _engines.pop(_engine_key(exchange, symbol), None)

    return {"ok": True, "message": "All grids stopped and open orders cancelled"}


async def _stop_grid(symbol: str, exchange: str | None = None):
    engine = _get_engine(exchange, symbol)
    if not _engine_requires_cleanup(engine):
        raise HTTPException(status_code=400, detail="No active grid")

    exchange = _engine_exchange(engine)
    await engine.stop()
    _upsert_grid_history(engine, "stopped")
    _delete_engine_state(symbol, exchange)
    with _engines_lock:
        _engines.pop(_engine_key(exchange, symbol), None)
    return {"ok": True, "message": f"{exchange.title()} {symbol} grid stopped and open orders cancelled"}


@app.get("/api/grid/status")
def grid_status():
    statuses = [_engine_status(engine) for engine in _engine_snapshot()]
    return {
        "running": any(status["running"] for status in statuses),
        "engine_count": len(statuses),
        "running_count": sum(1 for status in statuses if status["running"]),
        "engines": statuses,
    }


@app.get("/api/grid/status/{symbol}")
def grid_symbol_status(symbol: str, exchange: str | None = None):
    symbol = symbol.upper().strip()
    engine = _get_engine(exchange, symbol)
    if not engine:
        return {"running": False, "symbol": symbol, "exchange": _normalize_exchange(exchange or _active_exchange)}
    return _engine_status(engine)


def _engine_status(engine: GridEngine) -> dict:
    status = engine.get_status()
    exchange = _engine_exchange(engine)
    status["exchange"] = exchange
    account_unrealised_pnl = 0.0
    actual_position_net_qty = 0.0
    mark_price = None
    try:
        client = engine.client
        resp = client.get_positions(str(status.get("symbol", "")).upper())
        if resp.get("retCode") == 0:
            for item in resp.get("result", {}).get("list", []):
                side = str(item.get("side") or "")
                try:
                    size = float(item.get("size", 0) or 0)
                except (TypeError, ValueError):
                    size = 0.0
                if side == "Buy":
                    actual_position_net_qty += size
                elif side == "Sell":
                    actual_position_net_qty -= size
                try:
                    account_unrealised_pnl += float(item.get("unrealisedPnl", 0) or 0)
                except (TypeError, ValueError):
                    pass
                if mark_price is None:
                    try:
                        mark_price = float(item.get("markPrice", 0) or 0)
                    except (TypeError, ValueError):
                        mark_price = None
    except Exception:
        account_unrealised_pnl = 0.0

    realized_net = float(status.get("total_profit", 0) or 0)
    grid_unrealised_pnl = (
        engine.estimate_grid_unrealized_pnl(mark_price)
        if mark_price is not None and mark_price > 0
        else 0.0
    )
    expected_position_net_qty = float(status.get("expected_position_net_qty", 0) or 0)
    status["realized_gross_profit"] = status.get("gross_profit", 0)
    status["realized_net_profit"] = round(realized_net, 4)
    status["unrealised_pnl"] = round(grid_unrealised_pnl, 4)
    status["account_unrealised_pnl"] = round(account_unrealised_pnl, 4)
    status["account_position_net_qty"] = round(actual_position_net_qty, 8)
    status["position_delta_from_grid"] = round(actual_position_net_qty - expected_position_net_qty, 8)
    status["total_equity_profit"] = round(realized_net + grid_unrealised_pnl, 4)
    return status


@app.get("/api/grid/history")
def grid_history(limit: int = 100):
    try:
        history = _load_grid_history_file()
    except GridHistoryIntegrityError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    safe_limit = max(1, min(int(limit or 100), 500))
    runs = sorted(
        history.get("runs", []),
        key=lambda item: float(item.get("started_at") or 0),
        reverse=True,
    )[:safe_limit]
    return {"runs": runs}


def _is_grid_order(item: dict) -> bool:
    link_id = str(item.get("orderLinkId") or item.get("order_link_id") or "")
    return link_id.startswith(("g_", "open_", "init_", "repair_"))


def _managed_order_ids(engine: GridEngine | None) -> set[str]:
    if not engine:
        return set()
    ids = {str(order.get("order_id", "")) for order in engine.active_orders.values()}
    if engine.opening_order:
        ids.add(str(engine.opening_order.get("order_id", "")))
    pending_reduce_action = getattr(engine, "pending_reduce_action", None)
    if pending_reduce_action:
        ids.add(str(pending_reduce_action.get("order_id", "")))
    return {order_id for order_id in ids if order_id}


def _managed_order_links(engine: GridEngine | None) -> set[str]:
    if not engine:
        return set()
    links = {
        str(order.get("link_id", "") or "")
        for order in engine.active_orders.values()
    }
    if engine.opening_order:
        links.add(str(engine.opening_order.get("link_id", "") or ""))
    pending_reduce_action = getattr(engine, "pending_reduce_action", None)
    if pending_reduce_action:
        links.add(str(pending_reduce_action.get("link_id", "") or ""))
    return {link_id for link_id in links if link_id}


def _risk_snapshot(symbol: str, exchange: str | None = None) -> dict:
    exchange = _normalize_exchange(exchange or _active_exchange)
    client = _get_client(exchange)
    symbol = symbol.upper().strip()
    engine = _get_engine(exchange, symbol)
    managed_ids = _managed_order_ids(engine)
    managed_links = _managed_order_links(engine)

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
        and str(item.get("orderLinkId", "") or "") not in managed_links
    ]

    position_resp = client.get_positions(symbol)
    if position_resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=position_resp.get("retMsg", "Failed to fetch positions"))
    positions = []
    actual_position_net_qty = 0.0
    for item in position_resp["result"].get("list", []):
        try:
            size = float(item.get("size", 0))
        except (TypeError, ValueError):
            size = 0
        if size > 0:
            side = item.get("side", "")
            if side == "Buy":
                actual_position_net_qty += size
            elif side == "Sell":
                actual_position_net_qty -= size
            positions.append(
                {
                    "side": side,
                    "size": item.get("size", "0"),
                    "entry_price": item.get("avgPrice", "0"),
                    "mark_price": item.get("markPrice", "0"),
                    "unrealised_pnl": item.get("unrealisedPnl", "0"),
                }
            )

    unmanaged_position = bool(positions and not engine)
    unmanaged_delta_qty = 0.0
    expected_position_net_qty = 0.0
    reduce_protection = {}
    reduce_protection_risk = False
    grid_coverage = {}
    grid_coverage_risk = False
    pending_submissions = []
    queued_replacements = []
    accepted_shape_mismatches = []
    risk_shutdown_pending = False
    manual_stop_pending = False
    initialization_failed = False
    initialization_in_progress = False
    if engine:
        status = engine.get_status()
        expected_position_net_qty = float(status.get("expected_position_net_qty", 0) or 0)
        unmanaged_delta_qty = actual_position_net_qty - expected_position_net_qty
        unmanaged_position = engine._qty_reaches_accounting_step(unmanaged_delta_qty)
        reduce_protection = status.get("reduce_protection") or {}
        reduce_protection_risk = bool(reduce_protection.get("has_risk"))
        grid_coverage = status.get("grid_coverage") or {}
        grid_coverage_risk = bool(grid_coverage.get("has_risk"))
        submission_records = list(engine.active_orders.values())
        if engine.opening_order:
            submission_records.append(engine.opening_order)
        if engine.pending_reduce_action:
            submission_records.append(engine.pending_reduce_action)
        risk_shutdown_pending = bool(status.get("risk_shutdown_pending"))
        manual_stop_pending = bool(status.get("manual_stop_pending"))
        initialization_failed = bool(status.get("initialization_failed"))
        initialization_in_progress = bool(status.get("initialization_in_progress"))
        pending_submissions = [
            {
                "order_link_id": order.get("link_id", ""),
                "side": order.get("side", ""),
                "price": order.get("price", "0"),
                "qty": order.get("qty", "0"),
                "status": order.get("status", "SUBMIT_UNKNOWN"),
                "reduce_only": bool(order.get("reduce_only", False)),
                "attempts": int(order.get("submission_attempts", 1) or 1),
                "error": order.get("submission_error", ""),
            }
            for order in submission_records
            if order.get("submission_pending") or order is engine.pending_reduce_action
        ]
        queued_replacements = [
            {
                "mode": item.get("replacement_mode", "counter_order"),
                "side": item.get("side", ""),
                "price": item.get("price", "0"),
                "qty": item.get("qty", "0"),
                "level_idx": item.get("level_idx"),
                "reduce_only": bool(item.get("reduce_only", False)),
                "attempts": int(item.get("replacement_retry_attempts", 0) or 0),
            }
            for item in engine.paused_replacements
        ]
        accepted_shape_mismatches = [
            {
                "order_id": order.get("order_id", ""),
                "order_link_id": order.get("link_id", ""),
                "reason": order.get("accepted_shape_mismatch", ""),
                "expected_side": order.get("expected_side", order.get("side", "")),
                "expected_price": order.get("expected_price", order.get("price", "0")),
                "expected_qty": order.get("expected_qty", order.get("qty", "0")),
                "expected_reduce_only": bool(
                    order.get("expected_reduce_only", order.get("reduce_only", False))
                ),
                "actual_side": order.get("exchange_accepted_side", order.get("side", "")),
                "actual_price": order.get("exchange_accepted_price", order.get("price", "0")),
                "actual_qty": order.get("exchange_accepted_qty", order.get("qty", "0")),
                "actual_reduce_only": bool(
                    order.get(
                        "exchange_accepted_reduce_only",
                        order.get("reduce_only", False),
                    )
                ),
            }
            for order in submission_records
            if order.get("accepted_shape_mismatch")
        ]
    return {
        "symbol": symbol,
        "exchange": exchange,
        "engine_running": bool(engine and engine.running),
        "orphan_order_count": len(orphan_orders),
        "orphan_orders": orphan_orders,
        "unmanaged_position": unmanaged_position,
        "unmanaged_delta_qty": round(unmanaged_delta_qty, 8),
        "expected_position_net_qty": round(expected_position_net_qty, 8),
        "actual_position_net_qty": round(actual_position_net_qty, 8),
        "reduce_protection": reduce_protection,
        "grid_coverage": grid_coverage,
        "pending_submission_count": len(pending_submissions),
        "pending_submissions": pending_submissions,
        "queued_replacement_count": len(queued_replacements),
        "queued_replacements": queued_replacements,
        "accepted_shape_mismatch_count": len(accepted_shape_mismatches),
        "accepted_shape_mismatches": accepted_shape_mismatches,
        "risk_shutdown_pending": risk_shutdown_pending,
        "manual_stop_pending": manual_stop_pending,
        "initialization_failed": initialization_failed,
        "initialization_in_progress": initialization_in_progress,
        "state_store_error": _grid_state_integrity_error,
        "history_store_error": _grid_history_integrity_error,
        "positions": positions,
        "has_risk": bool(
            orphan_orders
            or unmanaged_position
            or reduce_protection_risk
            or grid_coverage_risk
            or pending_submissions
            or queued_replacements
            or accepted_shape_mismatches
            or risk_shutdown_pending
            or manual_stop_pending
            or initialization_failed
            or initialization_in_progress
            or _grid_state_integrity_error
            or _grid_history_integrity_error
        ),
    }


@app.get("/api/risk/{symbol}")
def risk_snapshot(symbol: str, exchange: str | None = None):
    return _risk_snapshot(symbol, exchange)


@app.post("/api/risk/cancel-orphans/{symbol}")
def cancel_orphan_orders(symbol: str, exchange: str | None = None):
    snapshot = _risk_snapshot(symbol, exchange)
    client = _get_client(snapshot["exchange"])
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
def order_history(symbol: str, exchange: str | None = None):
    client = _get_client(exchange)
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


@app.get("/api/trades/{symbol}")
def recent_trades(symbol: str, limit: int = 100, exchange: str | None = None):
    client = _get_client(exchange)
    if not hasattr(client, "get_recent_trades"):
        raise HTTPException(status_code=400, detail="Exchange client does not support trade history")

    safe_limit = max(1, min(int(limit or 100), 500))
    resp = client.get_recent_trades(symbol.upper(), safe_limit)
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to fetch trades"))

    trades = [
        {
            "order_id": item.get("orderId", ""),
            "trade_id": item.get("tradeId", ""),
            "side": item.get("side", ""),
            "price": item.get("price", "0"),
            "qty": item.get("qty", "0"),
            "volume": item.get("volume", "0"),
            "fee": item.get("fee", "0"),
            "fee_asset": item.get("feeAsset", ""),
            "fee_usdt": item.get("feeUsdt", ""),
            "realized_pnl": item.get("realizedPnl", "0"),
            "is_maker": item.get("isMaker", False),
            "time": item.get("time", ""),
        }
        for item in resp["result"].get("list", [])
    ]
    return {"trades": trades}


@app.get("/api/orders/open/{symbol}")
def open_orders(symbol: str, exchange: str | None = None):
    client = _get_client(exchange)
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
def cancel_all_symbol_orders(symbol: str, exchange: str | None = None):
    exchange = _normalize_exchange(exchange or _active_exchange)
    client = _get_client(exchange)
    resp = client.cancel_all_orders(symbol.upper())
    if resp.get("retCode") != 0:
        raise HTTPException(status_code=400, detail=resp.get("retMsg", "Failed to cancel open orders"))
    return {"ok": True, "message": f"All open orders for {exchange.title()} {symbol.upper()} were cancelled"}


def _get_client(exchange: str | None = None):
    return _client_for_exchange(exchange, require_config=True)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
