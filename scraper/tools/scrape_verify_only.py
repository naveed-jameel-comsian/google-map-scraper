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
import json
import os
import re
import time
from typing import List, Dict, Tuple, Set, Optional
from urllib.parse import urlparse, urljoin

import httpx

try:
    from core.telemetry import JsonLogger, new_run_id
except Exception:
    JsonLogger = None


    def new_run_id() -> str:
        return time.strftime("run_%Y%m%d_%H%M%S")
OUT_ROOT = os.getenv("OUT_ROOT", "/out")
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
    HUNTER_API_KEY = None

import os as _os

_env_key = _os.getenv("HUNTER_API_KEY")
if _env_key:
    HUNTER_API_KEY = _env_key
if HUNTER_API_KEY:
    masked = HUNTER_API_KEY[:4] + "…" if len(HUNTER_API_KEY) > 4 else "****"
    print(f"{ts()} | INFO  | HUNTER_API_KEY detected ({masked})")
else:
    print(f"{ts()} | WARN  | HUNTER_API_KEY missing → verification disabled")


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

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en,en-US;q=0.9",
}


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
    while attempt <= retries:
        try:
            r = await client.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=TIMEOUT,
                follow_redirects=True,
            )
            if r.status_code in (429, 408) or (500 <= r.status_code < 600):
                raise httpx.HTTPStatusError(
                    f"retryable status: {r.status_code}", request=r.request, response=r
                )

            if 200 <= r.status_code < 400:
                ct = r.headers.get("content-type", "").lower()
                if ct.startswith("text/html") or ct.startswith("application/xhtml"):
                    return r.text or ""
            return None

        except (httpx.RequestError, httpx.TimeoutException, httpx.HTTPStatusError):
            if attempt >= retries:
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 8.0)
            attempt += 1
        except Exception:
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


def extract_emails_with_context(html: str, page_url: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if not html:
        return out

    for m in MAILTO_RE.finditer(html):
        for raw in _clean_mailto_target(m.group(1)):
            raw = _normalize_email_text(raw)
            mm = EMAIL_RE.fullmatch(raw)
            if not mm:
                continue
            out.append((f"{mm.group(1)}@{mm.group(2)}", page_url))

    for m in EMAIL_RE.finditer(html):
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
        return None
    url = "https://api.hunter.io/v2/email-verifier"
    params = {"email": email, "api_key": HUNTER_API_KEY}
    backoff = 1.5
    tries = 0
    while True:
        tries += 1
        try:
            r = await client.get(url, params=params, timeout=30.0)
            if r.status_code == 200:
                data = r.json().get("data") or {}
                return {
                    "email": email,
                    "status": data.get("status"),
                    "score": data.get("score"),
                    "accept_all": data.get("accept_all"),
                    "disposable": data.get("disposable"),
                    "webmail": data.get("webmail"),
                    "mx_records": data.get("mx_records"),
                    "smtp_check": data.get("smtp_check"),
                }
            if r.status_code == 429:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)
                continue
            return None
        except Exception:
            if tries >= 3:
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 30.0)


async def scrape_site(start_url: str,
                      max_pages: int = MAX_PAGES,
                      include_external: bool = INCLUDE_EXTERNAL) -> List[Dict[str, str]]:
    root_domain = normalize_domain(start_url)
    if not root_domain:
        warn(f"cannot determine root domain for {start_url}")
        return []

    limits = httpx.Limits(max_connections=16, max_keepalive_connections=8)
    timeout = httpx.Timeout(connect=8.0, read=TIMEOUT, write=TIMEOUT, pool=8.0)

    queue: List[str] = [start_url]
    visited: Set[str] = set()
    pages_scanned = 0

    out: List[Dict[str, str]] = []
    seen_emails: Set[str] = set()

    info(f"scraping {start_url} ...")
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        while queue and pages_scanned < max_pages:
            url = queue.pop(0)
            if url in visited:
                continue
            if not include_external and not same_domain(url, root_domain):
                continue
            visited.add(url)

            html = await fetch(client, url)
            pages_scanned += 1
            if not html:
                continue

            for e, where in extract_emails_with_context(html, url):
                domain = e.lower().split("@", 1)[1]
                if domain == root_domain or domain.endswith("." + root_domain):
                    k = e.lower()
                    if k not in seen_emails and not _is_placeholder(e):
                        seen_emails.add(k)
                        out.append({"email": e, "found_on": where})

            links = extract_candidate_links(html, url, limit=40)
            if not include_external:
                links = [u for u in links if same_domain(u, root_domain)]
            for u in links:
                if u not in visited and u not in queue:
                    queue.append(u)

    return out


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
    Flatten verifier results rows into a CSV:
      name, website, verified_emails (semicolon-joined)
    """
    rows = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            with contextlib.suppress(Exception):
                obj = json.loads(line)
                name = obj.get("name") or ""
                website = obj.get("website") or ""
                verified = obj.get("verified_emails") or []
                rows.append({"name": name, "website": website, "verified_emails": ";".join(verified)})

    _ensure_dir(os.path.dirname(csv_path))
    with open(csv_path, "w", newline="", encoding="utf-8") as out:
        w = csv.DictWriter(out, fieldnames=["name", "website", "verified_emails"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


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

    sem = asyncio.Semaphore(args.site_concurrency)
    verified_total = 0
    processed = 0

    async def process_one(i: int, obj: dict):
        nonlocal verified_total, processed
        name = (obj.get("name") or "(unknown)") if isinstance(obj, dict) else "(unknown)"
        site = (obj.get("website") or "").strip() if isinstance(obj, dict) else ""
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

            info("verifying scraped emails (cache + Hunter for misses) ...")
            verified = await verify_scraped_emails(scraped, concurrency=args.verify_concurrency,
                                                   use_cache=bool(args.use_hunter_cache))
            print("\nVerified (Hunter):")
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
            _emails_jsonl_to_csv(run_emails_path, files["emails_csv"])
            info(f"emails CSV exported → {files['emails_csv']}")
            csv_export_ok = True
        except Exception as e:
            warn(f"emails CSV export failed: {e}")

        finished_ts = ts()

        _append_meta(
            meta_path,
            status="done",
            phase="emails_scraped" if processed > 0 else "emails_scraping_failed",
            finished_at_emails=finished_ts,
            finished_at=finished_ts,
            email_counters={"sites_processed": processed, "emails_verified": verified_total},
            files=files
        )
        if logger:
            logger.info("[emails] job.done", processed=processed, emails_verified=verified_total)

        if csv_export_ok:
            emails_jsonl_path = files.get("emails_jsonl")
            if emails_jsonl_path:
                try:
                    if os.path.exists(emails_jsonl_path):
                        os.remove(emails_jsonl_path)
                        info(f"deleted emails JSONL → {emails_jsonl_path}")
                except Exception as e:
                    warn(f"failed to delete emails JSONL {emails_jsonl_path}: {e}")

            if args.infile:
                try:
                    if os.path.exists(args.infile):
                        os.remove(args.infile)
                        info(f"deleted input JSONL → {args.infile}")
                except Exception as e:
                    warn(f"failed to delete input JSONL {args.infile}: {e}")

        finished_ts = ts()

        _append_meta(
            meta_path,
            status="done",
            phase="emails_scraped" if processed > 0 else "emails_scraping_failed",
            finished_at_emails=finished_ts,
            finished_at=finished_ts,
            email_counters={"sites_processed": processed, "emails_verified": verified_total},
            files=files
        )
        if logger:
            logger.info("[emails] job.done", processed=processed, emails_verified=verified_total)

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


if __name__ == "__main__":
    main()
