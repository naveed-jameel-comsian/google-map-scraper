from __future__ import annotations
import os

try:
    import local_settings as LS
except Exception:
    LS = None


def _get(name: str, default=None):
    if name in os.environ:
        return os.environ[name]
    if LS and hasattr(LS, name):
        return getattr(LS, name)
    return default


SMARTPROXY_HOST = _get("SMARTPROXY_HOST")
SMARTPROXY_PORT = _get("SMARTPROXY_PORT")
SMARTPROXY_USER = _get("SMARTPROXY_USER")
SMARTPROXY_PASSWORD = _get("SMARTPROXY_PASSWORD")

SMARTPROXY_CC = _get("SMARTPROXY_CC")

PROXY_URL = _get("PROXY_URL")
DEFAULT_DELAY_MIN = float(_get("DEFAULT_DELAY_MIN", 0.8))
DEFAULT_DELAY_MAX = float(_get("DEFAULT_DELAY_MAX", 1.6))
DEFAULT_PROXY_MODE = _get("DEFAULT_PROXY_MODE", "sticky")


def has_smartproxy_creds() -> bool:
    return bool(SMARTPROXY_HOST and SMARTPROXY_PORT and SMARTPROXY_USER and SMARTPROXY_PASSWORD)
