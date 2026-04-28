import base64
import ctypes
import os
import sys
from ctypes import wintypes


CRYPTPROTECT_UI_FORBIDDEN = 0x01


class DataBlob(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    ]


def _raise_last_error():
    raise ctypes.WinError(ctypes.get_last_error())


def _get_fernet():
    key = os.getenv("GRID_CONFIG_KEY", "").strip()
    if not key:
        raise RuntimeError("GRID_CONFIG_KEY is required for encrypted config storage on servers")

    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError("cryptography is required for GRID_CONFIG_KEY encrypted storage") from exc

    return Fernet(key.encode("ascii"))


def _protect_bytes(data: bytes) -> bytes:
    input_buffer = ctypes.create_string_buffer(data)
    input_blob = DataBlob(
        len(data),
        ctypes.cast(input_buffer, ctypes.POINTER(ctypes.c_byte)),
    )
    output_blob = DataBlob()

    result = ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(input_blob),
        None,
        None,
        None,
        None,
        CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(output_blob),
    )
    if not result:
        _raise_last_error()

    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(output_blob.pbData)


def _unprotect_bytes(data: bytes) -> bytes:
    input_buffer = ctypes.create_string_buffer(data)
    input_blob = DataBlob(
        len(data),
        ctypes.cast(input_buffer, ctypes.POINTER(ctypes.c_byte)),
    )
    output_blob = DataBlob()

    result = ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(input_blob),
        None,
        None,
        None,
        None,
        CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(output_blob),
    )
    if not result:
        _raise_last_error()

    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(output_blob.pbData)


def encrypt_text(value: str) -> str:
    if os.getenv("GRID_CONFIG_KEY", "").strip():
        return _get_fernet().encrypt(value.encode("utf-8")).decode("ascii")
    if sys.platform != "win32":
        raise RuntimeError("Secure file storage needs GRID_CONFIG_KEY outside Windows")

    encrypted = _protect_bytes(value.encode("utf-8"))
    return base64.b64encode(encrypted).decode("ascii")


def decrypt_text(value: str) -> str:
    if os.getenv("GRID_CONFIG_KEY", "").strip():
        return _get_fernet().decrypt(value.encode("ascii")).decode("utf-8")
    if sys.platform != "win32":
        raise RuntimeError("Secure file storage needs GRID_CONFIG_KEY outside Windows")

    encrypted = base64.b64decode(value.encode("ascii"))
    return _unprotect_bytes(encrypted).decode("utf-8")


def storage_backend() -> str:
    if os.getenv("GRID_CONFIG_KEY", "").strip():
        return "fernet"
    if sys.platform == "win32":
        return "windows-dpapi"
    return "unavailable"
