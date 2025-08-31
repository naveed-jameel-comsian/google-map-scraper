from __future__ import annotations

import random
import string
from typing import Optional, Dict

from core.config import (
    SMARTPROXY_HOST, SMARTPROXY_PORT, SMARTPROXY_USER, SMARTPROXY_PASSWORD,
    SMARTPROXY_CC, has_smartproxy_creds
)


def _rand_token(n: int = 12) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def new_session(mode: str = "sticky") -> Optional[str]:
    """
    Return a session token for sticky IPs.
    For rotating mode, return None so vendor rotates per-request.
    """
    if (mode or "").lower() == "sticky":
        return _rand_token(12)
    return None


def _compose_username(base_user: str, session: Optional[str]) -> str:
    """
    Many residential proxies accept session encoded into username.
    Adjust this pattern to match your Smartproxy account’s requirements.

    Common Smartproxy formats:
      - "<user>-session-<token>"
      - "customer-<user>-cc-<CC>-session-<token>"
    """
    parts = [base_user]
    if SMARTPROXY_CC:
        parts.append(f"cc-{SMARTPROXY_CC}")
    if session:
        parts.append(f"session-{session}")
    return "-".join(parts)


def playwright_proxy_config(session: Optional[str]) -> Optional[Dict[str, str]]:
    """
    Build Playwright `proxy=` dict. Returns None if creds are missing.
    """
    if not has_smartproxy_creds():
        return None

    host = str(SMARTPROXY_HOST).strip()
    port = str(SMARTPROXY_PORT).strip()

    if not host or not port:
        return None

    server = f"http://{host}:{port}"
    username = _compose_username(SMARTPROXY_USER, session)
    password = SMARTPROXY_PASSWORD

    return {
        "server": server,
        "username": username,
        "password": password,
    }
