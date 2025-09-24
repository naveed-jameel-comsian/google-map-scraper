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
    Adjust this pattern to match your Smartproxy account's requirements.

    Common Smartproxy formats:
      - "<user>-session-<token>"
      - "customer-<user>-cc-<CC>-session-<token>"
      - Just the base user (for some plans)
    """
    # For Smartproxy, use simple username format that works with requests
    return base_user


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


def get_alternative_proxy_configs(session: Optional[str]) -> List[Dict[str, str]]:
    """
    Get alternative proxy configurations to try if the primary one fails.
    Common Smartproxy ports: 7000, 7001, 7002, 3120, 3128
    """
    if not has_smartproxy_creds():
        return []
    
    host = str(SMARTPROXY_HOST).strip()
    if not host:
        return []
    
    # Common ports for Smartproxy
    alt_ports = ["7000", "7001", "7002", "3120", "3128", "8080"]
    primary_port = str(SMARTPROXY_PORT).strip()
    
    configs = []
    for port in alt_ports:
        if port == primary_port:
            continue  # Skip the primary port we already tried
            
        server = f"http://{host}:{port}"
        username = _compose_username(SMARTPROXY_USER, session)
        password = SMARTPROXY_PASSWORD
        
        configs.append({
            "server": server,
            "username": username,
            "password": password,
            "port": port
        })
    
    return configs
