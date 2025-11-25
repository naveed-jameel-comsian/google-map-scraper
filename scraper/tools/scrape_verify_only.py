"""
Scrape a site for emails (prioritizing header/footer/nav/contact-ish links),
then verify scraped emails via Hunter (valid + accept_all). Prints to terminal.

Usage:
  # single site
  python scrape_verify_only.py --url https://www.atriushealth.org/ --run-id <RUN_ID>

  # loop a GMaps JSONL (expects each line to have {"website": "...", "name": "..."}):
  python scrape_verify_only.py --infile out/gmaps_*.jsonl --run-id <RUN_ID>

Requirements:
  pip install httpx
"""

import argparse
import asyncio
import contextlib
import csv
import html
import json
import os
import re
import sys
import time
from typing import List, Dict, Tuple, Set, Optional
from urllib.parse import urlparse, urljoin, unquote

import httpx

# Ensure the parent `scraper` directory is on the path so `core.*` imports work
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, ".."))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

try:
    from core.telemetry import JsonLogger, new_run_id
except Exception:
    JsonLogger = None


    def new_run_id() -> str:
        return time.strftime("run_%Y%m%d_%H%M%S")

try:
    from core.mongodb import get_emails_for_domain, store_emails_for_domain, is_domain_cached, check_emails_exist_batch, store_emails
    MONGODB_AVAILABLE = True
except Exception:
    MONGODB_AVAILABLE = False
    store_emails = None
OUT_ROOT = os.getenv("OUT_ROOT") or os.path.abspath("out")
MAX_PAGES = 20
TIMEOUT = 15.0
INCLUDE_EXTERNAL = False
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

VERIFIER_CACHE_PATH = os.path.join(OUT_ROOT, "hunter_verifier_cache.jsonl")
VERIFIER_CACHE_LOCK = asyncio.Lock()

RESULTS_FILE_LOCK = asyncio.Lock()
DEFAULT_RESULTS_PATH = os.path.join(OUT_ROOT, "scrape_verify_results.jsonl")

_PLACEHOLDER_SUBSTR = (
    "example@", "your.name@", "firstname.lastname@", "first.last@", "name@domain.com",
    "test@", "admin@example.com", "user@example.com"
)


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


try:
    from local_settings import HUNTER_API_KEY
except Exception:
    HUNTER_API_KEY = "b46ad573287aeb978253e1d0f875acd93dca52e2"

import os as _os

_env_key = _os.getenv("HUNTER_API_KEY")
if _env_key:
    HUNTER_API_KEY = _env_key
if HUNTER_API_KEY:
    masked = HUNTER_API_KEY[:4] + "…" if len(HUNTER_API_KEY) > 4 else "****"
    print(f"{ts()} | INFO  | HUNTER_API_KEY detected ({masked})")
else:
    print(f"{ts()} | WARN  | HUNTER_API_KEY missing -> verification disabled")


def info(msg: str) -> None:
    print(f"{ts()} | INFO  | {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"{ts()} | WARN  | {msg}", flush=True)


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _write_json(path: str, obj: dict) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _append_meta(path: str, **kv) -> None:
    print(f"[DEBUG] _append_meta writing to {os.path.abspath(path)} with keys={list(kv.keys())}")
    meta = {}
    if os.path.exists(path):
        with contextlib.suppress(Exception):
            with open(path, "r", encoding="utf-8") as f:
                meta = json.load(f)
    meta.update(kv)
    _write_json(path, meta)


def _safe_load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def normalize_domain(url: str) -> Optional[str]:
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        netloc = urlparse(url).netloc.split("@")[-1].split(":")[0].lower().lstrip(".")
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc or None
    except Exception:
        return None


def normalize_domain_for_hunter(url: str) -> Optional[str]:
    """
    Extract base domain for Hunter API queries.
    Converts subdomains to base domain (e.g., locator.lacounty.gov -> lacounty.gov)
    """
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        netloc = urlparse(url).netloc.split("@")[-1].split(":")[0].lower().lstrip(".")
        if netloc.startswith("www."):
            netloc = netloc[4:]
        
        # Extract base domain by taking the last two parts (domain.tld)
        # This handles cases like locator.lacounty.gov -> lacounty.gov
        parts = netloc.split(".")
        if len(parts) >= 2:
            # For domains with 2+ parts, take the last 2 parts as base domain
            base_domain = ".".join(parts[-2:])
            return base_domain
        else:
            return netloc or None
    except Exception:
        return None


def extract_base_url(url: str) -> Optional[str]:
    """
    Extract base URL (protocol + domain) from a full URL with path.
    Example: https://www.thrivepetcare.com/locations/california/woodland-hills -> https://www.thrivepetcare.com
    """
    if not url:
        return None
    try:
        if url.startswith(("http://", "https://")):
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            return base_url
        else:
            # If it doesn't start with protocol, assume it's just a domain
            return url.lower().strip()
    except Exception:
        return None


def same_domain(url: str, root_domain: str) -> bool:
    try:
        d = normalize_domain(url)
        return d == root_domain or (d and d.endswith("." + root_domain))
    except Exception:
        return False


def absolutize(base: str, href: str) -> str:
    return href if href.startswith(("http://", "https://")) else urljoin(base, href)


EMAIL_RE = re.compile(r"(?i)(?<![A-Z0-9._%+-])([A-Z0-9._%+-]+)@([A-Z0-9.-]+\.[A-Z]{2,})(?![A-Z0-9._%+-])")
MAILTO_RE = re.compile(r'(?i)mailto:([^"\'<>\s]+)')
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)

HINTS = (
    "contact", "contact-us", "about", "about-us", "team", "staff", "leadership",
    "directory", "location", "locations", "appointment",
    "privacy", "terms", "policy", "policies", "support",
    "help", "media", "press"
)

# Public email domains we also want to capture during scraping
# Extend this list as needed (e.g., "yahoo.com", "outlook.com", etc.)
PUBLIC_EMAIL_DOMAINS = {
    "gmail.com",
    "outlook.com",
    "hotmail.com",
    "yahoo.com",
    "aol.com",
    "icloud.com",
}

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en,en-US;q=0.9",
}

# heree
async def fetch(client: httpx.AsyncClient, url: str, retries: int = 2) -> Optional[str]:
    """
    Fetch a page (HTML/XHTML only) with up to `retries` re-attempts on transient errors.
    Retries on:
      - network/timeout errors
      - 5xx responses
      - 429 (rate limited) and 408 (request timeout)
    """
    backoff = 1.5
    attempt = 0
    print(f"{ts()} | DEBUG   | [fetch] starting fetch for {url}")
    
    while attempt <= retries:
        try:
            print(f"{ts()} | DEBUG   | [fetch] attempt {attempt + 1}/{retries + 1} for {url}")
            r = await client.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=TIMEOUT,
                follow_redirects=True,
            )
            
            print(f"{ts()} | DEBUG   | [fetch] response status {r.status_code} for {url}")
            
            if r.status_code in (429, 408) or (500 <= r.status_code < 600):
                print(f"{ts()} | WARN    | [fetch] retryable status {r.status_code} for {url}")
                raise httpx.HTTPStatusError(
                    f"retryable status: {r.status_code}", request=r.request, response=r
                )

            if 200 <= r.status_code < 400:
                ct = r.headers.get("content-type", "").lower()
                print(f"{ts()} | DEBUG   | [fetch] content-type: {ct} for {url}")
                if ct.startswith("text/html") or ct.startswith("application/xhtml"):
                    content_len = len(r.text or "")
                    print(f"{ts()} | DEBUG   | [fetch] success: {content_len} chars for {url}")
                    return r.text or ""
                else:
                    print(f"{ts()} | WARN    | [fetch] non-HTML content-type {ct} for {url}")
            else:
                print(f"{ts()} | WARN    | [fetch] non-success status {r.status_code} for {url}")
            return None

        except (httpx.RequestError, httpx.TimeoutException, httpx.HTTPStatusError) as e:
            print(f"{ts()} | ERROR   | [fetch] request error (attempt {attempt + 1}): {type(e).__name__}: {e} for {url}")
            if attempt >= retries:
                print(f"{ts()} | ERROR   | [fetch] max retries reached for {url}")
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 8.0)
            attempt += 1
        except Exception as e:
            print(f"{ts()} | ERROR   | [fetch] unexpected error: {type(e).__name__}: {e} for {url}")
            import traceback
            print(f"{ts()} | DEBUG   | [fetch] traceback: {traceback.format_exc()}")
            return None
    return None


from urllib.parse import unquote as _unquote


def _clean_mailto_target(raw: str) -> List[str]:
    addr, _, qs = raw.partition("?")
    emails = [_unquote(p.strip()) for p in addr.split(",") if p.strip()]
    if qs:
        for part in qs.split("&"):
            k, _, v = part.partition("=")
            if k.lower() in {"to", "cc", "bcc"} and v:
                emails.extend([_unquote(p.strip()) for p in v.split(",") if p.strip()])
    return emails


def _normalize_email_text(e: str) -> str:
    e = _unquote(e).strip().strip("<>").strip('"\'')

    while e and e[0] in "%,;:()[]{}<> ":
        e = e[1:]
    return e


def extract_emails_with_context(html_content: str, page_url: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if not html_content:
        return out

    # Decode HTML entities in the content first
    decoded_html = html.unescape(html_content)

    for m in MAILTO_RE.finditer(decoded_html):
        for raw in _clean_mailto_target(m.group(1)):
            raw = _normalize_email_text(raw)
            mm = EMAIL_RE.fullmatch(raw)
            if not mm:
                continue
            out.append((f"{mm.group(1)}@{mm.group(2)}", page_url))

    for m in EMAIL_RE.finditer(decoded_html):
        out.append((f"{m.group(1)}@{m.group(2)}", page_url))

    seen = set()
    uniq = []
    for e, u in out:
        key = e.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append((e, u))
    return uniq


def extract_candidate_links(html: str, base_url: str, limit: int = 40) -> List[str]:
    if not html:
        return []
    scored = []
    low = html.lower()
    for m in HREF_RE.finditer(html):
        href = m.group(1).strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        absu = absolutize(base_url, href)

        start = max(0, m.start() - 120)
        end = min(len(html), m.end() + 120)
        ctx = low[start:end]

        score = 0
        if any(h in href.lower() for h in HINTS):
            score += 3
        if any(h in ctx for h in HINTS):
            score += 2
        if "<header" in ctx or "<footer" in ctx or "<nav" in ctx:
            score += 2

        scored.append((score, absu))

    scored.sort(key=lambda t: t[0], reverse=True)

    seen = set()
    out = []
    for s, u in scored:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= limit:
            break
    return out


def _is_placeholder(email: str) -> bool:
    e = email.lower().strip()
    return any(p in e for p in _PLACEHOLDER_SUBSTR)


def _load_verifier_cache() -> Dict[str, Dict]:
    if not os.path.exists(VERIFIER_CACHE_PATH):
        return {}
    cache = {}
    with open(VERIFIER_CACHE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            with contextlib.suppress(Exception):
                obj = json.loads(line)
                k = (obj.get("email") or "").lower()
                if k:
                    cache[k] = obj
    return cache


def _save_verifier_cache(entries: List[Dict]) -> None:
    if not entries:
        return
    _ensure_dir(os.path.dirname(VERIFIER_CACHE_PATH))
    with open(VERIFIER_CACHE_PATH, "a", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


async def hunter_verify(client: httpx.AsyncClient, email: str) -> Optional[Dict]:
    if not HUNTER_API_KEY:
        print(f"{ts()} | WARN    | [hunter] no API key configured for {email}")
        return None
    
    url = "https://api.hunter.io/v2/email-verifier"
    params = {"email": email, "api_key": HUNTER_API_KEY}
    backoff = 1.5
    tries = 0
    
    print(f"{ts()} | DEBUG   | [hunter] verifying {email}")
    
    while True:
        tries += 1
        try:
            print(f"{ts()} | DEBUG   | [hunter] attempt {tries} for {email}")
            r = await client.get(url, params=params, timeout=30.0)
            
            print(f"{ts()} | DEBUG   | [hunter] response status {r.status_code} for {email}")
            
            if r.status_code == 200:
                data = r.json().get("data") or {}
                status = data.get("status")
                print(f"{ts()} | DEBUG   | [hunter] success for {email}: status={status}, score={data.get('score')}")
                return {
                    "email": email,
                    "status": status,
                    "score": data.get("score"),
                    "accept_all": data.get("accept_all"),
                    "disposable": data.get("disposable"),
                    "webmail": data.get("webmail"),
                    "mx_records": data.get("mx_records"),
                    "smtp_check": data.get("smtp_check"),
                }
            
            if r.status_code == 429:
                print(f"{ts()} | WARN    | [hunter] rate limited for {email}, backing off {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)
                continue
            
            if r.status_code == 401:
                print(f"{ts()} | ERROR   | [hunter] unauthorized (check API key) for {email}")
                return None
            
            if r.status_code == 403:
                print(f"{ts()} | ERROR   | [hunter] forbidden (quota exceeded?) for {email}")
                return None
            
            print(f"{ts()} | ERROR   | [hunter] unexpected status {r.status_code} for {email}")
            try:
                error_data = r.json()
                print(f"{ts()} | DEBUG   | [hunter] error response: {error_data}")
            except Exception:
                print(f"{ts()} | DEBUG   | [hunter] non-JSON error response")
            return None
            
        except httpx.TimeoutException:
            print(f"{ts()} | ERROR   | [hunter] timeout for {email} (attempt {tries})")
            if tries >= 3:
                print(f"{ts()} | ERROR   | [hunter] max retries reached for {email}")
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 30.0)
        except Exception as e:
            print(f"{ts()} | ERROR   | [hunter] unexpected error for {email}: {type(e).__name__}: {e}")
            import traceback
            print(f"{ts()} | DEBUG   | [hunter] traceback: {traceback.format_exc()}")
            if tries >= 3:
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 30.0)

async def scrape_site_with_hunter(start_url: str, limit: int = 50) -> List[Dict[str, str]]:
    """
    Use Hunter Domain Search to fetch company emails for the site's root domain.
    Returns a list of {"email": str, "found_on": str} similar to scrape_site().
    """
    root_domain = normalize_domain_for_hunter(start_url)
    if not root_domain:
        warn(f"cannot determine root domain for {start_url}")
        return []

    if not HUNTER_API_KEY:
        warn("[hunter] no API key configured; domain search skipped")
        return []

    url = "https://api.hunter.io/v2/domain-search"
    params = {
        "domain": root_domain,
        "api_key": HUNTER_API_KEY,
        "limit": max(1, min(int(limit or 50), 100)),
    }

    out: List[Dict[str, str]] = []
    seen: Set[str] = set()

    limits = httpx.Limits(max_connections=8, max_keepalive_connections=4)
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        try:
            info(f"[hunter] domain search for {root_domain}")
            r = await client.get(url, params=params)
            if r.status_code != 200:
                warn(f"[hunter] domain search failed: status {r.status_code} for {root_domain}")
                with contextlib.suppress(Exception):
                    dbg = r.json()
                    print(f"{ts()} | DEBUG   | [hunter] domain search error: {dbg}")
                return []

            data = r.json().get("data") or {}
            emails = data.get("emails") or []
            for item in emails:
                value = (item.get("value") or "").strip()
                if not value:
                    continue
                k = value.lower()
                # Exclude emails with unwanted prefixes
                excluded_prefixes = [
                    'release@', 'recruitment@', 'newsletter@', 'foundation@', 'publicrelations@',
                    'reviews@', 'resources@', 'opt-out@', 'medicalrecords@', 'publicaffairs@',
                    'referrals@', 'wordpress@', 'complaint@', 'support@', 'comments@',
                    'patientexperience@', 'compliance@', 'complianceofficer@', 'authorization@',
                    'privacy@', 'volunteers@', 'volunteer@', 'admissions@',
                    'webmaster@','hipaa.security@','patientaccounts@','workcomp@','seminairs@','humanresources@',
                    'records@','email@','covid@','MyChartSupport@','feedback@','healthinsurancehelp@','registration@',
                    'giving@','events@','knowyourstatus@','memberservices@','webmarketsonline@'
                ]
                if (k in seen or _is_placeholder(value) or k.endswith('.gov') or 
                    k.endswith('@squarespace.com') or 
                    any(k.startswith(prefix) for prefix in excluded_prefixes)):
                    continue
                srcs = item.get("sources") or []
                where = None
                if srcs and isinstance(srcs, list):
                    # pick the first still_on_page if available, else first uri
                    chosen = None
                    for s in srcs:
                        if isinstance(s, dict) and s.get("still_on_page") and s.get("uri"):
                            chosen = s
                            break
                    if not chosen:
                        chosen = srcs[0] if isinstance(srcs[0], dict) else None
                    if chosen and chosen.get("uri"):
                        where = chosen.get("uri")
                if not where:
                    where = f"hunter:domain-search:{root_domain}"

                seen.add(k)
                out.append({"email": value, "found_on": where})

        except httpx.TimeoutException:
            warn(f"[hunter] domain search timeout for {root_domain}")
        except Exception as e:
            print(f"{ts()} | ERROR   | [hunter] domain search error for {root_domain}: {type(e).__name__}: {e}")
            import traceback
            print(f"{ts()} | DEBUG   | [hunter] traceback: {traceback.format_exc()}")

    return out

async def scrape_site(start_url: str,
                      max_pages: int = MAX_PAGES,
                      include_external: bool = INCLUDE_EXTERNAL) -> List[Dict[str, str]]:
    root_domain = normalize_domain(start_url)
    if not root_domain:
        warn(f"cannot determine root domain for {start_url}")
        return []

    # First check MongoDB cache
    # if MONGODB_AVAILABLE:
    #     try:
    #         info(f"checking MongoDB cache for {root_domain} ...")
    #         cached_emails = await get_emails_for_domain(root_domain)
    #         if cached_emails:
    #             info(f"MongoDB cache hit: found {len(cached_emails)} emails for {root_domain}")
    #             # Convert cached emails back to the expected format
    #             return [{"email": email_doc["email"], "found_on": email_doc.get("found_on", "cached")} 
    #                    for email_doc in cached_emails]
    #         else:
    #             info(f"MongoDB cache miss for {root_domain}, proceeding with scraping...")
    #     except Exception as e:
    #         warn(f"MongoDB cache check failed for {root_domain}: {e}")
    #         info(f"Proceeding with scraping despite MongoDB error...")

    # First try Hunter domain search
    info(f"trying Hunter domain search for {root_domain} ...")
    hunter_emails = await scrape_site_with_hunter(start_url, limit=50)
    hunter_count = len(hunter_emails) if hunter_emails else 0
    info(f"Hunter found {hunter_count} emails for {root_domain}")

    # Also do web scraping to get additional emails
    info(f"web scraping {start_url} for additional emails...")

    limits = httpx.Limits(max_connections=16, max_keepalive_connections=8)
    timeout = httpx.Timeout(connect=8.0, read=TIMEOUT, write=TIMEOUT, pool=8.0)

    queue: List[str] = [start_url]
    visited: Set[str] = set()
    pages_scanned = 0

    out: List[Dict[str, str]] = []
    seen_emails: Set[str] = set()
        
    # Add Hunter emails to our results and seen set
    if hunter_emails:
        for item in hunter_emails:
            email = item["email"]
            k = email.lower()
            # Exclude emails with unwanted prefixes
            excluded_prefixes = [
                'release@', 'recruitment@', 'newsletter@', 'foundation@', 'publicrelations@',
                'reviews@', 'resources@', 'opt-out@', 'medicalrecords@', 'publicaffairs@',
                'referrals@', 'wordpress@', 'complaint@', 'support@', 'comments@',
                'patientexperience@', 'compliance@', 'complianceofficer@', 'authorization@',
                'privacy@', 'volunteers@', 'volunteer@', 'admissions@',
                'webmaster@','hipaa.security@','patientaccounts@','workcomp@','seminairs@','humanresources@',
                'records@','email@','covid@','MyChartSupport@','feedback@','healthinsurancehelp@','registration@',
                'giving@','events@','knowyourstatus@','memberservices@','webmarketsonline@'
            ]
            if (k not in seen_emails and not _is_placeholder(email) and not k.endswith('.gov') and 
                not k.endswith('@squarespace.com') and 
                not any(k.startswith(prefix) for prefix in excluded_prefixes)):
                seen_emails.add(k)
                out.append(item)

    info(f"scraping {start_url} ...")
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        while queue and pages_scanned < max_pages:
            url = queue.pop(0)
            if url in visited:
                continue
            if not include_external and not same_domain(url, root_domain):
                continue
            visited.add(url)

            html = await fetch(client, url) # heree
            pages_scanned += 1
            if not html:
                continue

            for e, where in extract_emails_with_context(html, url):
                domain = e.lower().split("@", 1)[1]
                is_same_domain = (domain == root_domain or domain.endswith("." + root_domain))
                is_public_domain = domain in PUBLIC_EMAIL_DOMAINS
                if is_same_domain or is_public_domain:
                    k = e.lower()
                    # Exclude emails with unwanted prefixes
                    excluded_prefixes = [
                        'release@', 'recruitment@', 'newsletter@', 'foundation@', 'publicrelations@',
                        'reviews@', 'resources@', 'opt-out@', 'medicalrecords@', 'publicaffairs@',
                        'referrals@', 'wordpress@', 'complaint@', 'support@', 'comments@',
                        'patientexperience@', 'compliance@', 'complianceofficer@', 'authorization@',
                        'privacy@', 'volunteers@', 'volunteer@', 'admissions@',
                        'webmaster@','hipaa.security@','patientaccounts@','workcomp@','seminairs@','humanresources@',
                        'records@','email@','covid@','MyChartSupport@','feedback@','healthinsurancehelp@','registration@',
                        'giving@','events@','knowyourstatus@','memberservices@','webmarketsonline@'
                    ]
                    if (k not in seen_emails and not _is_placeholder(e) and not k.endswith('.gov') and 
                        not k.endswith('@squarespace.com') and 
                        not any(k.startswith(prefix) for prefix in excluded_prefixes)):
                        seen_emails.add(k)
                        out.append({"email": e, "found_on": where})

            links = extract_candidate_links(html, url, limit=40)
            if not include_external:
                links = [u for u in links if same_domain(u, root_domain)]
            for u in links:
                if u not in visited and u not in queue:
                    queue.append(u)
    
    # First: de-duplication (case-insensitive) to ensure unique emails
    uniq_seen: Set[str] = set()
    uniq_out: List[Dict[str, str]] = []
    for item in out:
        em = (item.get("email") or "").strip()
        # URL-decode to handle %-encoded characters (e.g., %20 for space, %40 for @)
        em = unquote(em)
        # Remove any % characters that might remain after decoding
        em = em.replace('%', '')
        em = em.strip()
        # Store cleaned email in item
        item["email"] = em
        em_lower = em.lower()
        if em_lower and em_lower not in uniq_seen:
            # Exclude emails with unwanted prefixes and domains
            excluded_prefixes = [
                'release@', 'recruitment@', 'newsletter@', 'foundation@', 'publicrelations@',
                'reviews@', 'resources@', 'opt-out@', 'medicalrecords@', 'publicaffairs@',
                'referrals@', 'wordpress@', 'complaint@', 'support@', 'comments@',
                'patientexperience@', 'compliance@', 'complianceofficer@', 'authorization@',
                'privacy@', 'volunteers@', 'volunteer@', 'admissions@',
                'webmaster@','hipaa.security@','patientaccounts@','workcomp@','seminairs@','humanresources@',
                'records@','email@','covid@','MyChartSupport@','feedback@','healthinsurancehelp@','registration@',
                'giving@','events@','knowyourstatus@','memberservices@','webmarketsonline@'
            ]
            if (em_lower.endswith('.gov') or em_lower.endswith('@squarespace.com') or 
                any(em_lower.startswith(prefix) for prefix in excluded_prefixes)):
                continue
            uniq_seen.add(em_lower)
            uniq_out.append(item)
    
    # Second: check unique emails against MongoDB database (batch operation)
    if MONGODB_AVAILABLE and uniq_out:
        try:
            info(f"checking {len(uniq_out)} unique emails against database...")
            emails_to_check = [item.get("email", "") for item in uniq_out if item.get("email")]
            existing_emails = await check_emails_exist_batch(emails_to_check)
            existing_count = sum(existing_emails.values())
            
            if existing_count > 0:
                info(f"found {existing_count} emails already in database, filtering them out...")
                # Filter out emails that exist in database
                filtered_out: List[Dict[str, str]] = []
                for item in uniq_out:
                    em = (item.get("email") or "").lower()
                    if not existing_emails.get(em, False):
                        filtered_out.append(item)
                uniq_out = filtered_out
                info(f"after filtering: {len(uniq_out)} new emails remaining")
            else:
                info(f"no existing emails found in database, all {len(uniq_out)} emails are new")
            
            # Store new emails to emails collection for future checks
            if uniq_out and store_emails:
                try:
                    info(f"storing {len(uniq_out)} new emails to emails collection...")
                    stored_count = await store_emails(uniq_out, source="scraper")
                    if stored_count > 0:
                        info(f"successfully stored {stored_count} new emails to emails collection")
                    else:
                        warn(f"failed to store new emails to emails collection")
                except Exception as e:
                    warn(f"MongoDB email store failed: {e}, continuing without storing...")
        except Exception as e:
            warn(f"MongoDB email check failed: {e}, continuing without check...")
            # Continue with all unique emails if check fails

    web_count = max(0, len(uniq_out) - hunter_count)
    info(f"combined results: {hunter_count} from Hunter + {web_count} from web scraping = {len(uniq_out)} unique emails")
    
    return uniq_out

async def verify_scraped_emails(scraped: List[Dict[str, str]],
                                concurrency: int = 3,
                                use_cache: bool = True) -> List[str]:
    """
    - If use_cache=True: cache hits are trusted (skipped), but we still refresh
      their cache rows by appending an updated entry with a new ts.
    - If use_cache=False: verify everything fresh (no skipping), but write results
      to cache so the next run can hit the cache.
    Returns: list of verified emails ("valid" or "accept_all", non-disposable).
    """
    if not scraped:
        return []

    emails = [x["email"] for x in scraped if not _is_placeholder(x["email"])]
    if not emails:
        return []

    cache = _load_verifier_cache()

    if use_cache:
        to_verify = [e for e in emails if e.lower() not in cache]
        hit_emails = [e for e in emails if e.lower() in cache]
        info(f"  cache hit={len(hit_emails)}  cache miss={len(to_verify)}")
    else:
        to_verify = list(emails)
        hit_emails = []
        info(f"  cache bypassed: verifying all {len(to_verify)}")
    to_write: List[Dict] = []

    if use_cache and hit_emails:
        now_ts = int(time.time())
        for e in hit_emails:
            k = e.lower()
            v = cache.get(k)
            if not v:
                continue
            entry = {
                "email": k,
                "status": v.get("status"),
                "score": v.get("score"),
                "accept_all": v.get("accept_all"),
                "disposable": v.get("disposable"),
                "webmail": v.get("webmail"),
                "mx_records": v.get("mx_records"),
                "smtp_check": v.get("smtp_check"),
                "ts": now_ts,
            }
            cache[k] = entry
            to_write.append(entry)
    results: Dict[str, Dict] = {}
    if to_verify and HUNTER_API_KEY:
        sem = asyncio.Semaphore(concurrency)
        async with httpx.AsyncClient() as client:
            async def one(e: str):
                async with sem:
                    data = await hunter_verify(client, e)
                    results[e.lower()] = data or {"email": e.lower(), "status": None}

            tasks = [asyncio.create_task(one(e)) for e in to_verify]
            if tasks:
                printed = -1
                total = len(tasks)
                while True:
                    done, pending = await asyncio.wait(tasks, timeout=1.0, return_when=asyncio.FIRST_COMPLETED)
                    finished = sum(1 for t in tasks if t.done())
                    if finished != printed:
                        info(f"  verify progress: {finished}/{total}")
                        printed = finished
                    if not pending:
                        break
                with contextlib.suppress(Exception):
                    await asyncio.gather(*tasks, return_exceptions=True)

        now_ts = int(time.time())
        for k, v in results.items():
            entry = {
                "email": k,
                "status": v.get("status") if v else None,
                "score": v.get("score") if v else None,
                "accept_all": v.get("accept_all") if v else None,
                "disposable": v.get("disposable") if v else None,
                "webmail": v.get("webmail") if v else None,
                "mx_records": v.get("mx_records") if v else None,
                "smtp_check": v.get("smtp_check") if v else None,
                "ts": now_ts,
            }
            cache[k] = entry
            to_write.append(entry)  # append to file
        info(f"  hunter verified={len(results)} newly fetched")

    if to_write:
        async with VERIFIER_CACHE_LOCK:
            _save_verifier_cache(to_write)

    ok: List[str] = []
    for e in emails:
        k = e.lower()
        v = cache.get(k) or results.get(k)
        if not v:
            continue
        if v.get("disposable"):
            continue
        status = v.get("status")
        if status in ("valid", "accept_all"):
            ok.append(e)

    seen = set()
    uniq_ok = []
    for e in ok:
        k = e.lower()
        if k not in seen:
            seen.add(k)
            uniq_ok.append(e)
    return uniq_ok


def _append_jsonl(path: str, obj: dict) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _emails_jsonl_to_csv(jsonl_path: str, csv_path: str) -> None:
    """
    Flatten verifier results rows into a CSV where each email gets its own row:
      name, website, verified_emails (each email in separate row)
    """
    rows = []
    
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            with contextlib.suppress(Exception):
                obj = json.loads(line)
                name = obj.get("name") or ""
                website = obj.get("website") or ""
                # Extract base URL (protocol + domain) from full URL
                normalized_website = extract_base_url(website) if website else ""
                verified = obj.get("verified_emails") or []

                # Create a row for each email
                for i, email in enumerate(verified):
                    if i == 0:
                        # First email gets name and website
                        rows.append({
                            "name": name,
                            "website": normalized_website,
                            "verified_emails": email
                        })
                    else:
                        # Subsequent emails have empty name and website
                        rows.append({
                            "name": "",
                            "website": "",
                            "verified_emails": email
                        })
    
    # Create fieldnames: name, website, verified_emails
    fieldnames = ["name", "website", "verified_emails"]
    
    _ensure_dir(os.path.dirname(csv_path))
    with open(csv_path, "w", newline="", encoding="utf-8") as out:
        w = csv.DictWriter(out, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _export_unique_emails_csv(run_emails_jsonl: str, unique_csv_path: str) -> int:
    """
    Read per-site results JSONL and export a flat, de-duplicated list of verified emails.
    CSV columns: email, source_site, source_name
    Returns number of unique emails exported.
    """
    uniq: dict[str, Tuple[str, str]] = {}
    with open(run_emails_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            with contextlib.suppress(Exception):
                obj = json.loads(line)
                site = (obj.get("website") or "").strip()
                name = (obj.get("name") or "").strip()
                for e in (obj.get("verified_emails") or []):
                    k = e.lower()
                    # Exclude emails with unwanted prefixes and domains
                    excluded_prefixes = [
                        'release@', 'recruitment@', 'newsletter@', 'foundation@', 'publicrelations@',
                        'reviews@', 'resources@', 'opt-out@', 'medicalrecords@', 'publicaffairs@',
                        'referrals@', 'wordpress@', 'complaint@', 'support@', 'comments@',
                        'patientexperience@', 'compliance@', 'complianceofficer@', 'authorization@',
                        'privacy@', 'volunteers@', 'volunteer@', 'admissions@',
                        'webmaster@','hipaa.security@','patientaccounts@','workcomp@','seminairs@','humanresources@',
                        'records@','email@','covid@','MyChartSupport@','feedback@','healthinsurancehelp@','registration@',
                        'giving@','events@','knowyourstatus@','memberservices@','webmarketsonline@'
                    ]
                    if (k and k not in uniq and not k.endswith('.gov') and 
                        not k.endswith('@squarespace.com') and 
                        not any(k.startswith(prefix) for prefix in excluded_prefixes)):
                        uniq[k] = (site, name)

    _ensure_dir(os.path.dirname(unique_csv_path))
    with open(unique_csv_path, "w", newline="", encoding="utf-8") as out:
        w = csv.DictWriter(out, fieldnames=["email", "source_site", "source_name"])
        w.writeheader()
        for email, (site, name) in sorted(uniq.items()):
            w.writerow({"email": email, "source_site": site, "source_name": name})
    return len(uniq)


def parse_args():
    p = argparse.ArgumentParser(description="Scrape emails from site(s) and verify via Hunter (cached).")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--url", help="Single website to scrape/verify.")
    g.add_argument("--infile", help="GMaps/Yelp JSONL (expects 'website' and optional 'name').")

    p.add_argument("--max-pages", type=int, default=MAX_PAGES, help="Per-site crawl page cap.")
    p.add_argument("--site-concurrency", type=int, default=12,
                   help="How many sites to process at once when using --infile.")
    p.add_argument("--out", help="Append results as JSONL to this file (created if missing).",
                   default=DEFAULT_RESULTS_PATH)
    p.add_argument("--append", action="store_true",
                   help="Append to existing results file instead of clearing it.")
    p.add_argument("--include-external", action="store_true", help="Allow off-domain links (default: off).")
    p.add_argument("--verify-concurrency", type=int, default=3, help="Parallel Hunter verifications.")

    p.add_argument("--run-id", type=str, default=None, help="Join an existing run id (for meta.json/log coherence).")
    p.add_argument("--run-registry-dir", type=str,
                   default=os.path.join(OUT_ROOT, "runs"),
                   help="Root directory that holds run folders (default: OUT_ROOT/runs).")

    p.add_argument("--use-hunter-cache", type=int, default=1, dest="use_hunter_cache",
                   help="1=use cache for hits; 0=verify all fresh (cache still updated)"
                   )
    return p.parse_args()


def _update_live_meta(meta_path: str, files: Dict, processed: int, verified_total: int,
                      phase: Optional[str] = None) -> None:
    kv = {
        "status": "running",
        "last_heartbeat": ts(),
        "email_counters": {"sites_processed": processed, "emails_verified": verified_total},
        "files": files,
    }
    if phase:
        kv["phase"] = phase
    _append_meta(meta_path, **kv)


async def run_single(url: str, args, logger, run_emails_path: str, meta_path: str, files: Dict) -> int:
    site = url.strip()
    if not site.startswith(("http://", "https://")):
        site = "http://" + site

    scraped = await scrape_site(site, max_pages=args.max_pages, include_external=args.include_external)

    print("\nEmails found:")
    if scraped:
        for item in scraped:
            print(f"  - {item['email']}  (found on: {item['found_on']})")
    else:
        print("  (none)")

    if logger:
        logger.info("[emails] verifying.start", site=site, scraped=len(scraped))
    verified = await verify_scraped_emails(scraped, concurrency=args.verify_concurrency,
                                           use_cache=bool(args.use_hunter_cache))
    print("\nVerified (Hunter):")
    for e in verified:
        print(f"  - {e}")

    result_row = {
        "name": "(single-run)",
        "website": site,
        "scraped_emails": scraped,
        "verified_emails": verified
    }
    async with RESULTS_FILE_LOCK:
        _append_jsonl(args.out, result_row)
        _append_jsonl(run_emails_path, result_row)
        _update_live_meta(meta_path, files, processed=1, verified_total=len(verified), phase="scraping_emails")

    if logger:
        logger.info("[emails] verifying.done", site=site, verified=len(verified))
    return len(verified)


async def run_file(infile: str, args, logger, run_emails_path: str, meta_path: str, files: Dict) -> Tuple[int, int]:
    with open(infile, "r", encoding="utf-8") as f:
        lines = f.readlines()

    total = len(lines)
    info(f"loaded {total} records from {infile}")
    if logger:
        logger.info("[emails] infile.loaded", total=total, infile=infile)

    sem = asyncio.Semaphore(20)
    verified_total = 0
    processed = 0

    meta = _safe_load_json(meta_path)
    search_term = meta.get("search_term", "")
    search_location = meta.get("search_location", "")

    async def process_one(i: int, obj: dict): # gmap.jsonl > emails.jsonl
        nonlocal verified_total, processed
        name = (obj.get("name") or "(unknown)") if isinstance(obj, dict) else "(unknown)"
        site = (obj.get("website") or "").strip() if isinstance(obj, dict) else ""

        # Extract additional fields from gmaps data
        rating = obj.get("rating") if isinstance(obj, dict) else None
        reviews_count = obj.get("reviews_count") if isinstance(obj, dict) else None
        address = obj.get("address") if isinstance(obj, dict) else None
        phone = obj.get("phone") if isinstance(obj, dict) else None
        latitude = obj.get("latitude") if isinstance(obj, dict) else None
        longitude = obj.get("longitude") if isinstance(obj, dict) else None
        listing_url = obj.get("listing_url") if isinstance(obj, dict) else None


        if not site:
            info(f"[{i}/{total}] {name} | (no site)")
            return

        if not site.startswith(("http://", "https://")):
            site = "http://" + site

        async with sem:
            info(f"[{i}/{total}] {name} | {site}")
            if logger:
                logger.info("[emails] site.start", idx=i, total=total, name=name, site=site)

            scraped = await scrape_site(site, max_pages=args.max_pages, include_external=args.include_external)

            print("\nEmails found:")
            if scraped:
                for item in scraped:
                    print(f"  - {item['email']}  (found on: {item['found_on']})")
            else:
                print("  (none)")

            # verified = await verify_scraped_emails(scraped, concurrency=args.verify_concurrency,
            #                                        use_cache=bool(args.use_hunter_cache))
            info("skipping Hunter verification - using scraped emails directly ...")
            # Use scraped emails directly without Hunter verification
            verified = [item['email'] for item in scraped]
            print("\nScraped emails (no verification):")
            if verified:
                for e in verified:
                    print(f"  - {e}")
            else:
                print("  (none)")

            result_row = {
                "name": name,
                "website": site,
                "scraped_emails": scraped,
                "verified_emails": verified
            }

            # Store enhanced scraped emails in MongoDB if available
            if MONGODB_AVAILABLE:
                try:
                    root_domain = normalize_domain(site)
                    if root_domain:
                        info(f"storing {len(scraped)} emails in MongoDB cache for {root_domain}...")
                        success = await store_emails_for_domain(name, root_domain, scraped, 
                                                              search_term, search_location,source="gmaps", 
                                                              rating=rating, reviews_count=reviews_count, 
                                                              address=address, phone=phone, 
                                                              latitude=latitude, longitude=longitude, 
                                                              listing_url=listing_url)
                        if success:
                            info(f"successfully cached emails for {root_domain} in MongoDB")
                        else:
                            warn(f"failed to cache emails for {root_domain} in MongoDB")
                except Exception as e:
                    warn(f"MongoDB cache store failed for {root_domain}: {e}")


            async with RESULTS_FILE_LOCK:
                _append_jsonl(args.out, result_row)
                _append_jsonl(run_emails_path, result_row)

                processed += 1
                verified_total += len(verified)
                _update_live_meta(meta_path, files, processed=processed, verified_total=verified_total,
                                  phase="scraping_emails")

            if logger:
                logger.info("[emails] site.done", idx=i, name=name, verified=len(verified))

    tasks = []
    for i, line in enumerate(lines, 1):
        obj = {}
        with contextlib.suppress(Exception):
            obj = json.loads(line)
        tasks.append(asyncio.create_task(process_one(i, obj)))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    info(f"wrote result to {args.out}")
    return processed, verified_total


def main():
    args = parse_args()

    if args.out and not args.append:
        if os.path.exists(args.out):
            open(args.out, "w").close()
            info(f"cleared {args.out} at start of run")
    run_id = args.run_id or new_run_id()
    run_dir_root = args.run_registry_dir or "out/runs"
    run_dir = os.path.join(run_dir_root, run_id)
    _ensure_dir(run_dir)

    meta_path = os.path.join(run_dir, "meta.json")
    files = _safe_load_json(meta_path).get("files", {})
    files.setdefault("emails_jsonl", os.path.join(run_dir, "emails.jsonl"))
    files.setdefault("emails_csv", os.path.join(run_dir, "emails.csv"))
    files.setdefault("unique_emails_csv", os.path.join(run_dir, "unique_emails.csv"))

    out_dir = os.path.dirname(args.out) or "out"
    logger = JsonLogger(out_dir, run_id) if JsonLogger else None
    if logger:
        logger.info("[emails] job.start", run_id=run_id, out=args.out)

    _append_meta(
        meta_path,
        run_id=run_id,
        status="running",
        phase="scraping_emails",
        last_heartbeat=ts(),
        files=files,
        email_phase={"out": args.out}
    )

    run_emails_path = files["emails_jsonl"]

    print(f"[DEBUG] verifier run_dir = {os.path.abspath(run_dir)}")
    print(f"[DEBUG] verifier meta_path = {os.path.abspath(meta_path)}")
    print(f"[DEBUG] verifier emails_jsonl = {os.path.abspath(run_emails_path)}")
    print(f"[DEBUG] verifier emails_csv = {os.path.abspath(files['emails_csv'])}")

    processed = 0
    verified_total = 0
    try:
        if args.url:
            verified_total = asyncio.run(
                run_single(args.url, args, logger, run_emails_path, meta_path, files)
            )
            processed = 1 if verified_total >= 0 else 0
        else:
            processed, verified_total = asyncio.run(
                run_file(args.infile, args, logger, run_emails_path, meta_path, files)
            )
        csv_export_ok = False
        try:
            info(f"emails CSV exported started----------------------")
            _emails_jsonl_to_csv(run_emails_path, files["emails_csv"])
            info(f"emails CSV exported -> {files['emails_csv']}")
            # nuniq = _export_unique_emails_csv(run_emails_path, files["unique_emails_csv"])
            # info(f"unique emails CSV exported ({nuniq}) -> {files['unique_emails_csv']}")
            csv_export_ok = True
        except Exception as e:
            warn(f"emails CSV export failed: {e}")

        finished_ts = ts()

        _append_meta(
            meta_path,
            status="done",
            phase="emails_scraped" if processed > 0 else "Found no new email",
            finished_at_emails=finished_ts,
            finished_at=finished_ts,
            email_counters={"sites_processed": processed, "emails_verified": verified_total},
            files=files
        )
        if logger:
            logger.info("[emails] job.done", processed=processed, emails_verified=verified_total)
        
        # Save query record to MongoDB (independent of CSV export)
        if MONGODB_AVAILABLE:
            try:
                from core.mongodb import save_query_record, reset_mongodb_connection
                
                # Reset MongoDB connection to ensure fresh state
                reset_mongodb_connection()
                
                # Read metadata to get search term and location
                meta = _safe_load_json(meta_path)
                search_term = meta.get("search_term", "")
                search_location = meta.get("search_location", "")
                started_at = meta.get("started_at", "")
                
                # Read emails directly from JSONL file (not dependent on CSV export)
                sites_data = []
                if run_emails_path and os.path.exists(run_emails_path):
                    try:
                        with open(run_emails_path, "r", encoding="utf-8") as f:
                            for line in f:
                                with contextlib.suppress(Exception):
                                    obj = json.loads(line)
                                    name = obj.get("name", "")
                                    website = obj.get("website", "")
                                    # Extract base URL (protocol + domain) from full URL
                                    normalized_website = extract_base_url(website) if website else ""
                                    verified_emails = obj.get("verified_emails", [])

                                    # Extract additional fields from gmaps data
                                    rating = obj.get("rating")
                                    reviews_count = obj.get("reviews_count")
                                    address = obj.get("address")
                                    phone = obj.get("phone")
                                    latitude = obj.get("latitude")
                                    longitude = obj.get("longitude")
                                    listing_url = obj.get("listing_url")
                                    
                                    # Only save records that have at least one email
                                    if verified_emails:
                                        sites_data.append({
                                            "name": name,
                                            "website": normalized_website,
                                            "emails": verified_emails,
                                            "email_count": len(verified_emails),
                                            "rating": rating,
                                            "reviews_count": reviews_count,
                                            "address": address,
                                            "phone": phone,
                                            "latitude": latitude,
                                            "longitude": longitude,
                                            "listing_url": listing_url
                                        })
                        info(f"Read {len(sites_data)} records from emails JSONL for database storage")
                    except Exception as e:
                        warn(f"Failed to read emails JSONL for database storage: {e}")
                
                # Save to database using a fresh event loop
                if sites_data:
                    try:
                        info(f"Attempting to save {len(sites_data)} business records to MongoDB...")
                        info(f"MongoDB config: uri={os.getenv('MONGODB_URI', 'mongodb://localhost:27017')}, db={os.getenv('MONGODB_DATABASE', 'email_scraper')}")
                        
                        # Create a new event loop for this operation
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        
                        # Enable logging for MongoDB operations
                        import logging
                        mongodb_logger = logging.getLogger('core.mongodb')
                        mongodb_logger.setLevel(logging.DEBUG)
                        if not mongodb_logger.handlers:
                            handler = logging.StreamHandler()
                            handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
                            mongodb_logger.addHandler(handler)
                        
                        try:
                            info(f"Calling save_query_record with run_id={run_id}, businesses={len(sites_data)}")
                            success = loop.run_until_complete(save_query_record(
                                run_id=run_id,
                                search_term=search_term,
                                search_location=search_location,
                                started_at=started_at,
                                finished_at=finished_ts,
                                sites=sites_data,
                                email_count = verified_total
                            ))
                            info(f"save_query_record returned: {success}")
                            
                            if success:
                                info(f"[SUCCESS] Query record saved to database: {run_id} ({len(sites_data)} businesses)")
                            else:
                                warn(f"[FAILED] Failed to save query record to database: {run_id} (save returned False)")
                                warn(f"   This usually means MongoDB connection failed. Check logs above for connection errors.")
                        finally:
                            loop.close()
                    except Exception as e:
                        warn(f"[ERROR] Error saving query record to database: {type(e).__name__}: {e}")
                        import traceback
                        warn(f"Traceback: {traceback.format_exc()}")
                else:
                    info("No emails to save to database (no businesses with emails found)")
            except Exception as e:
                warn(f"Error saving query record to database: {e}")

        # if csv_export_ok:
        #     emails_jsonl_path = files.get("emails_jsonl")
        #     if emails_jsonl_path:
        #         try:
        #             if os.path.exists(emails_jsonl_path):
        #                 os.remove(emails_jsonl_path)
        #                 info(f"deleted emails JSONL -> {emails_jsonl_path}")
        #         except Exception as e:
        #             warn(f"failed to delete emails JSONL {emails_jsonl_path}: {e}")

        #     if args.infile:
        #         try:
        #             if os.path.exists(args.infile):
        #                 os.remove(args.infile)
        #                 info(f"deleted input JSONL -> {args.infile}")
        #         except Exception as e:
        #             warn(f"failed to delete input JSONL {args.infile}: {e}")
                    
    except Exception as e:
        if logger:
            logger.warn("[emails] job.error", error=str(e))
        finished_ts = ts()
        _append_meta(
            meta_path,
            status="done",
            phase="emails_scraped" if processed > 0 else "emails_scraping_failed",
            finished_at_emails=finished_ts,
            finished_at=finished_ts,
            email_counters={"sites_processed": processed, "emails_verified": verified_total},
            error=str(e),
            files=files
        )
        raise
    finally:
        # Close MongoDB connection if available
        if MONGODB_AVAILABLE:
            try:
                from core.mongodb import close_mongodb_connection
                # Use a fresh event loop for cleanup
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(close_mongodb_connection())
                    info("MongoDB connection closed")
                finally:
                    loop.close()
            except Exception as e:
                warn(f"Error closing MongoDB connection: {e}")

# https://pchcinc.org
# https://www.stevenkamaramd.com
# http://www.angelesurgentcare.com
if __name__ == "__main__":
    main()
    # import asyncio
    # result = asyncio.run(scrape_site("https://mytexasmd.com/location-houston-willowbrook", max_pages=20, include_external=False))
    # print(result)
