import asyncio
import contextlib
import csv
import json
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, cast, Callable, Awaitable
from itertools import product
import requests
from urllib.parse import quote_plus, urlparse, parse_qsl, urlunparse, urlencode, urljoin

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from core.proxy import new_session, playwright_proxy_config, get_alternative_proxy_configs
from core.telemetry import JsonLogger, new_run_id
import shutil

HARD_WORKER_TIMEOUT = 90
OUT_ROOT = os.getenv("OUT_ROOT") or os.path.abspath("out")


def _normloc_for_filename(loc: str) -> str:
    return loc.replace(" ", "_").replace(",", "").lower()


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _write_meta(path: str, **kv):
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(kv, f, ensure_ascii=False, indent=2)


def _append_meta(path: str, **kv):
    meta = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            meta = {}
    meta.update(kv)
    _write_meta(path, **meta)


def _safe_load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _jsonl_to_csv(jsonl_path: str, csv_path: str) -> None:
    fields = set()
    lines = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                lines.append(obj)
                fields.update(obj.keys())
            except Exception:
                continue
    priority = ["source", "search_term", "search_location", "name", "website", "phone",
                "address", "rating", "reviews_count", "latitude", "longitude", "listing_url"]
    ordered = [c for c in priority if c in fields] + [c for c in sorted(fields) if c not in priority]
    _ensure_dir(os.path.dirname(csv_path))
    with open(csv_path, "w", newline="", encoding="utf-8") as out:
        w = csv.DictWriter(out, fieldnames=ordered)
        w.writeheader()
        for row in lines:
            try:
                w.writerow(row)
            except Exception:
                continue


@dataclass
class Metrics:
    queued: int = 0
    success: int = 0
    failed: int = 0
    retried: int = 0
    pages_clicked: int = 0


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


async def test_proxy_connection(proxy_cfg: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Test proxy connection by making a simple request to check IP.
    Returns (success, working_config) - if primary fails, tries alternative configs.
    """
    if not proxy_cfg:
        print(f"{ts()} | DEBUG   | [proxy_test] no proxy config provided")
        return True, None
    
    # Test primary config
    print(f"{ts()} | DEBUG   | [proxy_test] testing primary proxy: {proxy_cfg.get('server')}")
    success, _ = await _test_single_proxy_config(proxy_cfg)
    if success:
        return True, proxy_cfg
    
    # Try alternative configurations
    print(f"{ts()} | INFO    | [proxy_test] primary failed, trying alternative configs...")
    alt_configs = get_alternative_proxy_configs(None)  # No session for testing
    
    for alt_cfg in alt_configs:
        print(f"{ts()} | DEBUG   | [proxy_test] trying alternative: {alt_cfg.get('server')}")
        success, _ = await _test_single_proxy_config(alt_cfg)
        if success:
            print(f"{ts()} | INFO    | [proxy_test] found working config: {alt_cfg.get('server')}")
            return True, alt_cfg
    
    print(f"{ts()} | ERROR   | [proxy_test] all proxy configs failed")
    return False, None


async def _test_single_proxy_config(proxy_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Test a single proxy configuration.
    Returns (success, response_info)
    """
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=False, proxy=proxy_cfg)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                await page.goto("https://api.ip.cc", timeout=10000)
                content = await page.content()
                print(f"{ts()} | DEBUG   | [proxy_test] response: {content[:200]}...")
                
                if "ip" in content.lower():
                    print(f"{ts()} | INFO    | [proxy_test] proxy connection successful")
                    return True, content
                else:
                    print(f"{ts()} | ERROR   | [proxy_test] proxy response doesn't contain IP info")
                    return False, content
                    
            except Exception as e:
                print(f"{ts()} | ERROR   | [proxy_test] failed to load test page: {e}")
                return False, str(e)
            finally:
                await context.close()
                await browser.close()
                
    except Exception as e:
        print(f"{ts()} | ERROR   | [proxy_test] proxy test failed: {e}")
        return False, str(e)


def safe_int(x: Optional[str]) -> Optional[int]:
    if not x:
        return None
    try:
        return int(x.replace(",", "").strip())
    except Exception:
        return None


def to_jsonl(path: str, item: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def _sanitize_name(n: Optional[str]) -> Optional[str]:
    if not n:
        return None
    n = n.strip()
    if not n or n.lower() == "google maps":
        return None
    return n


_LATLNG_RE_1 = re.compile(r"/@(-?\d+\.\d+),(-?\d+\.\d+)", re.I)
_LATLNG_RE_2 = re.compile(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", re.I)
PLACE_ID_RE = re.compile(r"!1s([^!]+)")


def place_id_from_href(href: str) -> str:
    """Extract place id from href; fallback to full href if not found."""
    m = PLACE_ID_RE.search(href)
    return m.group(1) if m else href


def dedupe_hrefs(raw_hrefs: list[str]) -> list[str]:
    seen = set()
    unique = []
    for href in raw_hrefs:
        pid = place_id_from_href(href)
        if pid in seen:
            continue
        seen.add(pid)
        unique.append(href)
    return unique


def extract_lat_lng_from_url(url: str) -> Tuple[Optional[float], Optional[float]]:
    if not url:
        return (None, None)
    m = _LATLNG_RE_1.search(url)
    if m:
        try:
            return (float(m.group(1)), float(m.group(2)))
        except Exception:
            pass
    m = _LATLNG_RE_2.search(url)
    if m:
        try:
            return (float(m.group(1)), float(m.group(2)))
        except Exception:
            pass
    return (None, None)


def clean_address(addr: Optional[str]) -> Optional[str]:
    if not addr:
        return addr
    addr = addr.replace("", " ")
    addr = re.sub(r"[\u2000-\u200F\u202A-\u202E\u2066-\u2069]", "", addr)
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr


def _add_locale_qs(url: str) -> str:
    u = urlparse(url)

    q: Dict[str, str] = dict(parse_qsl(u.query, keep_blank_values=True))
    q.setdefault("hl", "en")
    q.setdefault("gl", "us")
    new_query = cast(str, urlencode(q, doseq=True, safe="", encoding="utf-8", errors="strict"))
    return urlunparse(u._replace(query=new_query))


def _fallback_parse_rating_reviews_from_html(html: str):
    rating = None
    reviews = None

    m = re.search(r'(?:Rated\s*)?([0-9]+(?:\.[0-9]+)?)\s*(?:stars?|[★⭐])', html, re.I)
    if m:
        try:
            rating = float(m.group(1))
        except Exception:
            rating = None

    m = re.search(r'\(([\d,]+)\)\s*reviews?', html, re.I) or re.search(r'([\d,]+)\s+reviews?', html, re.I)
    if m:
        try:
            reviews = int(m.group(1).replace(",", ""))
        except Exception:
            reviews = None

    return rating, reviews


SEL_RESULTS_FEED = "div[role='feed']"
SEL_RESULT_ANCHORS = (
    "a[href*='/maps/place'], "
    "a[href^='https://www.google.com/maps/place'], "
    "a[href^='https://maps.google.com/?cid']"
)

SEL_NAME_H1 = "h1"
SEL_RATING = "[aria-label*='stars']"
SEL_REVIEWS = "button[aria-label*='reviews'], span[aria-label*='reviews']"
SEL_ADDR = "[data-item-id='address'], button[aria-label*='address'], div[aria-label*='Address']"
SEL_PHONE_BTN = "[data-item-id^='phone'], button[aria-label*='Phone']"
SEL_WEBSITE = "a[data-item-id='authority'], a[aria-label*='Website']"

GMAPS_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def _proxy_from_args(args) -> Optional[Dict[str, Any]]:
    if not getattr(args, "use_proxy", 0):
        print(f"{ts()} | INFO    | [proxy] OFF")
        return None
    
    mode = getattr(args, "proxy_mode", None)
    print(f"{ts()} | DEBUG   | [proxy] mode={mode}")
    
    try:
        _ = new_session()
        print(f"{ts()} | DEBUG   | [proxy] session creation successful")
    except Exception as e:
        print(f"{ts()} | ERROR   | [proxy] session creation failed: {e}")
        pass
    
    try:
        cfg = playwright_proxy_config(mode)
        print(f"{ts()} | DEBUG   | [proxy] playwright_proxy_config returned: {cfg}")
        
        if not cfg or "server" not in cfg:
            print(f"{ts()} | ERROR   | [proxy] requested but credentials missing/invalid → running without proxy")
            print(f"{ts()} | DEBUG   | [proxy] cfg={cfg}, has_server={'server' in (cfg or {})}")
            return None
        
        user_hint = cfg.get("username", "")
        safe_user = (user_hint[:6] + "…") if user_hint else ""
        print(f"{ts()} | INFO    | [proxy] ON  server={cfg.get('server')} user={safe_user}")
        print(f"{ts()} | DEBUG   | [proxy] full config: server={cfg.get('server')}, username={cfg.get('username')}, has_password={bool(cfg.get('password'))}")
        return cfg
    except Exception as e:
        print(f"{ts()} | ERROR   | [proxy] requested but playwright_proxy_config raised: {e} → running without proxy")
        import traceback
        print(f"{ts()} | DEBUG   | [proxy] traceback: {traceback.format_exc()}")
        return None


def _proxy_with_session(args, session_tag: str) -> Optional[Dict[str, Any]]:
    """Clone base proxy cfg and add a session suffix so provider gives a sticky unique IP."""
    base = _proxy_from_args(args)
    if not base:
        return None
    cfg = dict(base)
    u = cfg.get("username")
    if u:
        cfg["username"] = f"{u}"
    return cfg


class BrowserPool:
    def __init__(self, pw, args, size: int):
        self.pw = pw
        self.args = args
        self.size = size
        self.browsers: List[Browser] = []
        self.contexts: List[BrowserContext] = []
        self.locks: List[asyncio.Semaphore] = []
        self._rr = 0

    async def start(self):
        for i in range(self.size):
            tag = f"w{i:03d}"
            proxy_cfg = _proxy_with_session(self.args, tag)
            # Stabilize Chromium on Linux/EC2
            browser: Browser = await self.pw.chromium.launch(
                headless=False,
                proxy=proxy_cfg,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox"],
            )
            ctx: BrowserContext = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                device_scale_factor=1.0,
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "User-Agent": GMAPS_DEFAULT_HEADERS["User-Agent"],
                },
            )
            ctx.set_default_timeout(8000)
            ctx.set_default_navigation_timeout(20000)
            self.browsers.append(browser)
            self.contexts.append(ctx)
            self.locks.append(asyncio.Semaphore(1))

    def pick(self) -> BrowserContext:
        ctx = self.contexts[self._rr % self.size]
        self._rr += 1
        return ctx

    async def close(self):
        for ctx in self.contexts:
            with contextlib.suppress(Exception):
                await ctx.close()
        for b in self.browsers:
            with contextlib.suppress(Exception):
                await b.close()


async def _scroll_results_feed(
        page: Page,
        delay_min: float,
        delay_max: float,
        target: int = 0,
        max_scrolls: int = 120,
        min_growth: int = 10,
        progress_cb: Optional[Callable[[int, int], Awaitable[None]]] = None,
) -> None:
    feed = page.locator(SEL_RESULTS_FEED)
    try:
        await feed.wait_for(state="visible", timeout=12000)
        try:
            for _ in range(20):
                if await page.locator(SEL_RESULT_ANCHORS).count() > 0:
                    break
                await _click_consent_if_present(page)
                with contextlib.suppress(Exception):
                    await page.mouse.wheel(0, 2000)
                await asyncio.sleep(0.3)
        except Exception:
            pass
    except Exception:
        pass

    first_count = -1
    last_count = -1
    stagnant_rounds = 0
    stagnant_limit = 30

    for i in range(max_scrolls):
        anchors = await page.locator(SEL_RESULT_ANCHORS).element_handles()
        count = len(anchors)
        print(f"{ts()} | INFO    | [gmaps] visible_anchors={count} scrolls={i}")
        if progress_cb:
            with contextlib.suppress(Exception):
                await progress_cb(count, i)

        if first_count < 0:
            first_count = count

        if target and count >= target:
            break

        if last_count == count:
            stagnant_rounds += 1
            if stagnant_rounds >= stagnant_limit:
                break
        else:
            stagnant_rounds = 0

        last_count = count

        try:
            if await feed.count() > 0:
                await feed.evaluate("el => el.scrollBy(0, el.scrollHeight)")
            else:
                await page.mouse.wheel(0, 4000)
        except Exception:
            await page.mouse.wheel(0, 4000)

        await asyncio.sleep(random.uniform(max(0.03, delay_min), max(0.22, delay_max)))

    if first_count >= 0 and last_count >= 0:
        total_growth = max(0, last_count - first_count)
        if total_growth < min_growth:
            return


async def _click_next_page_if_present(
        page: Page,
        *,
        grab_current_hrefs_async: Optional[Callable[[], Awaitable[List[str]]]] = None
) -> bool:
    """
    Try to click a 'Next page' control. If grab_current_hrefs_async is provided,
    wait until the results list actually changes before returning True.
    """
    selectors = [
        "button[aria-label*='Next page']",
        "div[role='button'][aria-label*='Next page']",
        "button[aria-label*='Next']",
        "div[role='button'][aria-label*='Next']",
    ]

    before_set = set(await grab_current_hrefs_async()) if grab_current_hrefs_async else None

    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=800):
                await btn.click(timeout=1500)
                await page.wait_for_load_state("networkidle", timeout=5000)
                await asyncio.sleep(0.8)
                if before_set is not None:
                    for _ in range(8):
                        after = set(await grab_current_hrefs_async())
                        if after and after != before_set:
                            return True
                        await asyncio.sleep(0.5)
                else:
                    return True
        except Exception:
            pass

    try:
        feed = page.locator(SEL_RESULTS_FEED).first
        await feed.focus(timeout=800)
        await page.keyboard.press("End")
        await page.wait_for_load_state("networkidle", timeout=3000)
        await asyncio.sleep(0.6)
        if grab_current_hrefs_async:
            after2 = set(await grab_current_hrefs_async())
            if before_set is None or after2 != before_set:
                return True
        else:
            return True
    except Exception:
        pass

    return False


async def _collect_anchor_hrefs(page: Page, limit: int, delay_min: float, delay_max: float,
                                progress_cb: Optional[Callable[[Dict[str, int]], Awaitable[None]]] = None) -> List[str]:
    per_page_target = 180 if not limit else min(200, max(80, int(limit * 0.8)))

    BASE = "https://www.google.com"

    async def grab() -> List[str]:
        els = await page.locator(SEL_RESULT_ANCHORS).element_handles()
        hrefs: List[str] = []
        for h in els:
            try:
                href = await h.get_attribute("href")
                if not href:
                    continue

                is_place_rel = href.startswith("/maps/place")
                is_place_abs = href.startswith("https://www.google.com/maps/place")
                is_cid = href.startswith("https://maps.google.com/?cid")

                if is_place_rel or is_place_abs or is_cid:
                    if href.startswith("/"):
                        href = urljoin(BASE, href)
                    hrefs.append(href)
            except Exception:
                pass
        return list(dict.fromkeys(hrefs))

    hrefs: List[str] = []
    pages_clicked = 0
    # Allow traversing many more pages to collect more than ~100 hrefs
    hard_cap_pages = 8

    prev_len = -1
    no_growth_passes = 0

    while (not limit or len(hrefs) < limit) and pages_clicked <= hard_cap_pages:
        async def _scroll_cb(visible_anchors: int, scrolls: int):
            if progress_cb:
                await progress_cb({
                    "visible_anchors": visible_anchors,
                    "scrolls": scrolls,
                    "hrefs_collected": len(hrefs),
                })

        await _scroll_results_feed(
            page,
            delay_min,
            delay_max,
            target=per_page_target,
            max_scrolls=120,
            min_growth=10,
            progress_cb=_scroll_cb,
        )

        current = await grab()
        if not hrefs and not current:
            print(f"{ts()} | WARN    | [gmaps] no anchors yet; url={page.url}")
        elif len(current) < 40:
            try:
                await page.keyboard.press("End")
                await asyncio.sleep(random.uniform(0.4, 0.8))
            except Exception:
                pass
        hrefs = list(dict.fromkeys(hrefs + current))
        hrefs = dedupe_hrefs(hrefs)
        print(f"{ts()} | INFO    | [gmaps] collected {len(hrefs)} hrefs (target limit={limit})")
        # Progress callback (hrefs tally)
        if progress_cb:
            with contextlib.suppress(Exception):
                await progress_cb({
                    "hrefs_collected": len(hrefs),
                })

        if limit and len(hrefs) >= limit:
            break

        if len(hrefs) == prev_len:
            no_growth_passes += 1
        else:
            no_growth_passes = 0
        prev_len = len(hrefs)

        clicked = await _click_next_page_if_present(
            page,
            grab_current_hrefs_async=grab
        )
        if clicked:
            pages_clicked += 1
            await _scroll_results_feed(
                page,
                delay_min,
                delay_max,
                target=per_page_target // 2,
                max_scrolls=40,
                min_growth=5,
                progress_cb=_scroll_cb,
            )
            await asyncio.sleep(random.uniform(0.5, 1.0))
            continue

        # Aggressive early-break: stop after 2 consecutive no-growth passes
        if no_growth_passes >= 1:
            break

    print(f"{ts()} | INFO    | [gmaps] final hrefs={len(hrefs)}")
    return hrefs


async def _click_consent_if_present(page: Page) -> None:
    """
    Try common consent/OAuth banners on Google domains.
    - Clicks visible buttons with names like Accept/I agree/Allow all.
    - Falls back to scanning iframes for such buttons.
    """
    selectors = [
        re.compile(r"(Accept all|Accept|I agree|I’m in|Yes, I’m in|Allow all)", re.I),
        re.compile(r"(AGREE|ACCEPT)", re.I),
    ]
    # Direct page first
    for rx in selectors:
        try:
            btn = page.get_by_role("button", name=rx).first
            if await btn.is_visible(timeout=1200):
                await btn.click(timeout=2000)
                await asyncio.sleep(0.2)
                return
        except Exception:
            pass

    # Some consent UIs are inside iframes
    try:
        for f in page.frames:
            if f is page.main_frame:
                continue
            for rx in selectors:
                try:
                    btn = f.get_by_role("button", name=rx).first
                    if await btn.is_visible(timeout=600):
                        await btn.click(timeout=1500)
                        await asyncio.sleep(0.2)
                        return
                except Exception:
                    continue
    except Exception:
        pass


async def _extract_text(page: Page, selector: str, timeout_ms: int = 2000) -> Optional[str]:
    try:
        t = await page.locator(selector).first.text_content(timeout=timeout_ms)
        if t:
            return t.strip()
    except Exception:
        return None
    return None
def _normalize_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "http://" + u
    try:
        netloc = urlparse(u).netloc.split("@")[-1].split(":")[0].lower().lstrip(".")
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc or None
    except Exception:
        return None


async def _extract_details(page: Page, href: str) -> Dict[str, Any]:
    name = await _extract_text(page, SEL_NAME_H1, 2500)
    name = _sanitize_name(name)

    rating_text = await _extract_text(page, SEL_RATING, 1500)
    rating = None
    if rating_text:
        m = re.search(r"(\d+(?:\.\d+)?)", rating_text)
        if m:
            try:
                rating = float(m.group(1))
            except Exception:
                rating = None

    reviews_text = await _extract_text(page, SEL_REVIEWS, 1500)
    reviews_count = None
    if reviews_text:
        m = re.search(r"(\d[\d,]*)", reviews_text)
        if m:
            reviews_count = safe_int(m.group(1))

    if rating is None or reviews_count is None:
        try:
            html = await page.content()
            fr, fc = _fallback_parse_rating_reviews_from_html(html)
            if rating is None:
                rating = fr
            if reviews_count is None:
                reviews_count = fc
        except Exception:
            pass

    address = await _extract_text(page, SEL_ADDR, 2000)
    address = clean_address(address)

    phone = None
    try:
        pbtn = page.locator(SEL_PHONE_BTN).first
        aria = await pbtn.get_attribute("aria-label", timeout=1500)
        if aria:
            m = re.search(r"(\+?\d[\d\-\(\)\s]+)", aria)
            if m:
                phone = m.group(1).strip()
        if not phone:
            phone = await pbtn.text_content(timeout=1500)
            if phone:
                phone = phone.strip()
    except Exception:
        pass

    website = None
    try:
        we = page.locator(SEL_WEBSITE).first
        website = await we.get_attribute("href", timeout=1500)
    except Exception:
        pass

    lat, lng = extract_lat_lng_from_url(page.url or href)

    if not name:
        try:
            h1 = await page.locator(SEL_NAME_H1).first.text_content(timeout=1200)
            name = _sanitize_name(h1)
        except Exception:
            pass

    return {
        "name": name,
        "rating": rating,
        "reviews_count": reviews_count,
        "address": address,
        "phone": phone,
        "website": website,
        "latitude": lat,
        "longitude": lng,
        "listing_url": href,
    }

SCREENSHOT_DIR = "screenshots"
# ensure screenshot folder exists
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

async def _process_href(ctx: BrowserContext, href: str, delay_min: float, delay_max: float) -> Optional[Dict[str, Any]]:
    page = await ctx.new_page()
    try:
        safe_href = _add_locale_qs(href)
        print(f"{ts()} | DEBUG   | [gmaps] open -> {safe_href[:120]}")
        await page.goto(safe_href, wait_until="domcontentloaded", timeout=60000) # heree
        await _click_consent_if_present(page)
        await asyncio.sleep(random.uniform(delay_min, delay_max))

        data = await _extract_details(page, safe_href)
        if not data or not data.get("name"):
            print(f"{ts()} | WARN    | [gmaps] no_data name-missing href={safe_href[:120]}")
            return None

        print(f"{ts()} | DEBUG   | [gmaps] ok: name={data.get('name')!r}")
        return data

    except Exception as e:
        try:
            now_url = page.url
        except Exception:
            now_url = "n/a"
        
        # Save screenshot on error
        # filename = href.replace("https://", "").replace("/", "_")[:100]  # sanitize filename
        # screenshot_path = os.path.join(SCREENSHOT_DIR, f"{filename}.png")
        # with contextlib.suppress(Exception):
        #     await page.screenshot(path=screenshot_path, full_page=True)
            
        print(
            f"{ts()} | ERROR   | [gmaps] detail-fail href={href[:120]} url_now={now_url[:120]} err={type(e).__name__}: {e}")
        return None
    finally:
        with contextlib.suppress(Exception):
            await page.close()


async def with_sem(sem, coro):
    await sem.acquire()
    try:
        return await coro
    finally:
        try:
            sem.release()
        except Exception:
            pass


async def run_gmaps(args) -> None:
    """
    Args expected (matching your CLI):
      args.q, args.location, args.limit, args.use_proxy, args.proxy_mode, args.concurrency, args.delay_min, args.delay_max
    Output: out/gmaps_{q}_{location}.jsonl
    """
    q = (args.q or "").strip()
    loc = (args.location or "").strip()
    limit = int(getattr(args, "limit", 0) or 0)  # 0 = all // heree
    concurrency = 1 # int(getattr(args, "concurrency", 8) or 5)
    dmin = float(getattr(args, "delay_min", 0.05) or 0.05)
    dmax = float(getattr(args, "delay_max", 0.15) or 0.15)
    ip_per_worker = 0 # int(getattr(args, "ip_per_worker", 0) or 0)

    q_full = f"{q} {loc}".strip()
    outfile = os.path.join(OUT_ROOT, f"gmaps_{q.replace(' ', '_')}_{_normloc_for_filename(loc)}.jsonl")
    run_id = getattr(args, "run_id", None) or new_run_id()
    run_dir_root = getattr(args, "run_registry_dir", None) or os.path.join(OUT_ROOT, "runs")
    run_dir = os.path.join(run_dir_root, run_id)
    _ensure_dir(run_dir)
    meta_path = os.path.join(run_dir, "meta.json")
    print(f"[DEBUG] gmaps run_dir = {os.path.abspath(run_dir)}")
    print(f"[DEBUG] gmaps meta_path = {os.path.abspath(meta_path)}")
    print(f"[DEBUG] gmaps dashboard.log path = {os.path.abspath(os.path.join(run_dir, 'dashboard.log'))}")
    print(f"[DEBUG] gmaps outfile = {os.path.abspath(outfile)}")

    outfile = os.path.join(OUT_ROOT, f"gmaps_{q.replace(' ', '_')}_{_normloc_for_filename(loc)}.jsonl")

    started_ts = ts()
    _write_meta(meta_path,
                run_id=run_id,
                started_at=started_ts,
                finished_at=None,
                status="running",
                phase="scraping_maps",
                source="gmaps",
                search_term=q,
                search_location=loc,
                limit=limit,
                concurrency=concurrency,
                delays={"min": dmin, "max": dmax},
                files={"gmaps_jsonl": os.path.join(run_dir, "gmaps.jsonl")},
                counters={"queued": 0, "written": 0, "failures": 0}
                )
    out_dir = OUT_ROOT
    logger = JsonLogger(out_dir, run_id)

    logger.info("[gmaps] job.start",
                q=q, location=loc, limit=limit, concurrency=concurrency,
                delays={"min": dmin, "max": dmax}, outfile=outfile)
    try:
        if os.path.exists(outfile):
            os.remove(outfile)
            print(f"{ts()} | INFO    | [gmaps] cleared existing {outfile}")
    except Exception as e:
        print(f"{ts()} | WARNING | [gmaps] could not clear {outfile}: {e}")

    start_url = _add_locale_qs(f"https://www.google.com/maps/search/{quote_plus(q_full)}")

    async with async_playwright() as pw:
        if ip_per_worker:
            print(f"{ts()} | DEBUG   | [gmaps] using ip_per_worker mode with {concurrency} workers")
            pool = BrowserPool(pw, args, concurrency)
            await pool.start()
            page: Page = await pool.contexts[0].new_page()
        else:
            proxy_cfg = _proxy_from_args(args)
            
            # Test proxy connection before proceeding
            if proxy_cfg:
                print(f"{ts()} | DEBUG   | [gmaps] testing proxy before starting...")
                proxy_working, working_config = await test_proxy_connection(proxy_cfg)
                if not proxy_working:
                    print(f"{ts()} | WARNING | [gmaps] proxy test failed, but continuing anyway...")
                else:
                    print(f"{ts()} | INFO    | [gmaps] proxy test passed")
                    # Use the working config if we found one
                    if working_config and working_config != proxy_cfg:
                        print(f"{ts()} | INFO    | [gmaps] using alternative proxy config: {working_config.get('server')}")
                        proxy_cfg = working_config
            
            print(f"{ts()} | DEBUG   | [gmaps] launching browser with proxy_cfg={bool(proxy_cfg)}")
            # Stabilize Chromium on Linux/EC2
            browser: Browser = await pw.chromium.launch(
                headless=False,
                proxy=proxy_cfg,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox"],
            )
            ctx: BrowserContext = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                device_scale_factor=1.0,
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "User-Agent": GMAPS_DEFAULT_HEADERS["User-Agent"],
                },
            )
            ctx.set_default_timeout(8000)
            ctx.set_default_navigation_timeout(15000)
            page: Page = await ctx.new_page()

        try:
            await page.goto(start_url, wait_until="domcontentloaded", timeout=45000)
            await _click_consent_if_present(page)
        except Exception:
            pass

        # Detect navigation failures early (commonly due to proxy misconfiguration)
        try:
            current_url = page.url
            print(f"{ts()} | DEBUG   | [gmaps] initial page load - url={current_url}")
        except Exception as e:
            current_url = ""
            print(f"{ts()} | ERROR   | [gmaps] failed to get page url: {e}")
        
        if not current_url or current_url.startswith("chrome-error://"):
            print(f"{ts()} | ERROR   | [gmaps] navigation failed url={current_url or '(empty)'}")
            print(f"{ts()} | DEBUG   | [gmaps] start_url was: {start_url}")
            print(f"{ts()} | DEBUG   | [gmaps] proxy was enabled: {getattr(args, 'use_proxy', 0)}")
            print(f"{ts()} | DEBUG   | [gmaps] ip_per_worker mode: {ip_per_worker}")
            
            # Log detailed error info
            try:
                page_content = await page.content()
                print(f"{ts()} | DEBUG   | [gmaps] page content length: {len(page_content)}")
                if "chrome-error" in page_content:
                    print(f"{ts()} | ERROR   | [gmaps] chrome-error page detected")
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    print(f"{ts()} | ERROR   | [gmaps] access blocked page detected")
            except Exception as e:
                print(f"{ts()} | DEBUG   | [gmaps] could not get page content: {e}")
            
            # Retry once without proxy if a proxy was requested and we're not in ip_per_worker mode
            if not ip_per_worker and getattr(args, "use_proxy", 0):
                print(f"{ts()} | INFO    | [gmaps] retrying without proxy due to navigation failure ...")
                try:
                    with contextlib.suppress(Exception):
                        await ctx.close()
                        print(f"{ts()} | DEBUG   | [gmaps] closed browser context")
                    with contextlib.suppress(Exception):
                        await browser.close()
                        print(f"{ts()} | DEBUG   | [gmaps] closed browser")
                    
                    print(f"{ts()} | DEBUG   | [gmaps] launching browser without proxy...")
                    browser = await pw.chromium.launch(
                        headless=False,
                        proxy=None,
                        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox"],
                    )
                    ctx = await browser.new_context(
                        viewport={"width": 1440, "height": 900},
                        device_scale_factor=1.0,
                        locale="en-US",
                        extra_http_headers={
                            "Accept-Language": "en-US,en;q=0.9",
                            "User-Agent": GMAPS_DEFAULT_HEADERS["User-Agent"],
                        },
                    )
                    ctx.set_default_timeout(8000)
                    ctx.set_default_navigation_timeout(15000)
                    page = await ctx.new_page()
                    print(f"{ts()} | DEBUG   | [gmaps] retrying navigation to {start_url}")
                    
                    try:
                        await page.goto(start_url, wait_until="domcontentloaded", timeout=45000)
                        await _click_consent_if_present(page)
                        current_url = page.url
                        print(f"{ts()} | DEBUG   | [gmaps] retry successful - url={current_url}")
                    except Exception as e:
                        current_url = ""
                        print(f"{ts()} | ERROR   | [gmaps] retry navigation failed: {e}")
                        import traceback
                        print(f"{ts()} | DEBUG   | [gmaps] retry traceback: {traceback.format_exc()}")
                except Exception as e:
                    print(f"{ts()} | ERROR   | [gmaps] browser retry setup failed: {e}")
                    import traceback
                    print(f"{ts()} | DEBUG   | [gmaps] browser retry traceback: {traceback.format_exc()}")
            
            if not current_url or current_url.startswith("chrome-error://"):
                print(f"{ts()} | ERROR   | [gmaps] still cannot load Google Maps. Check proxy creds or run with --use_proxy 0")
                print(f"{ts()} | DEBUG   | [gmaps] final current_url: {current_url}")
                logger.error("[gmaps] startup.navigation_failed", start_url=start_url, final_url=current_url)
                logger.close()
                # Mark run as failed and exit early
                _append_meta(meta_path,
                             status="done",
                             phase="maps_scraping_failed",
                             counters={"queued": 0, "written": 0, "failures": 0},
                             error=f"navigation_failed: {current_url}")
                if ip_per_worker:
                    await pool.close()
                else:
                    with contextlib.suppress(Exception):
                        await ctx.close()
                    with contextlib.suppress(Exception):
                        await browser.close()
                return

        # 1) Collect hrefs for the main query with progress updates to meta
        async def _progress_update(payload: Dict[str, int]) -> None:
            try:
                # Enrich with active query and cumulative hrefs
                payload2 = dict(payload)
                payload2["query"] = q_full
                payload2["hrefs_total"] = payload.get("hrefs_collected", 0)
                _append_meta(meta_path, gmaps_progress=payload2, last_heartbeat=ts())
            except Exception:
                pass

        hrefs = await _collect_anchor_hrefs(page, limit, dmin, dmax, progress_cb=_progress_update)
        hrefs = dedupe_hrefs(hrefs)

        # 2) If results are few and limit is high, broaden with query/location variants
        # def _singular_plural_variants(term: str) -> List[str]:
        #     t = (term or "").strip()
        #     out = {t}
        #     if not t:
        #         return []
        #     if t.endswith("ies"):
        #         out.add(t[:-3] + "y")
        #     if t.endswith("ses") or t.endswith("xes"):
        #         out.add(t[:-2])
        #     if t.endswith("s") and len(t) > 3:
        #         out.add(t[:-1])
        #     else:
        #         out.add(t + "s")
        #     return [x for x in out if x]

        # def _query_variants(base_q: str) -> List[str]:
        #     base_q = (base_q or "").strip()
        #     words = base_q.split()
        #     variants: List[str] = []
        #     # Fetch dynamic related terms via Datamuse (no API key). Best-effort; ignore on error.
        #     def _datamuse_terms(term: str) -> List[str]:
        #         try:
        #             resp = requests.get(
        #                 "https://api.datamuse.com/words",
        #                 params={"ml": term, "max": 8},
        #                 timeout=5.0,
        #             )
        #             if resp.status_code == 200:
        #                 data = resp.json() or []
        #                 out: List[str] = []
        #                 for obj in data:
        #                     w = (obj.get("word") or "").strip()
        #                     if w and 2 <= len(w) <= 40 and all(c.isalnum() or c.isspace() for c in w):
        #                         out.append(w)
        #                 return out[:6]
        #         except Exception:
        #             pass
        #         return []
        #     if base_q:
        #         variants.append(base_q)
        #     if 0 < len(words) <= 2:
        #         toggles: List[List[str]] = []
        #         for w in words:
        #             toggles.append(_singular_plural_variants(w))
        #         for combo in product(*toggles):
        #             variants.append(" ".join(combo))
        #     if len(words) == 1:
        #         key = words[0].lower().rstrip('s')
        #         for syn in _datamuse_terms(key):
        #             variants.append(syn)
        #     seen = set()
        #     out: List[str] = []
        #     for v in variants:
        #         v2 = " ".join(v.split())
        #         if v2 and v2.lower() not in seen:
        #             out.append(v2)
        #             seen.add(v2.lower())
        #     return out[:10]

        # def _location_variants(base_loc: str) -> List[str]:
        #     base_loc = (base_loc or "").strip()
        #     out: List[str] = []
        #     # Geocode and nearby cities via Nominatim/Overpass (best-effort)
        #     def _geocode(loc_text: str) -> Optional[Tuple[float, float]]:
        #         try:
        #             resp = requests.get(
        #                 "https://nominatim.openstreetmap.org/search",
        #                 params={"q": loc_text, "format": "json", "limit": 1},
        #                 headers={"User-Agent": "gmaps-scraper/1.0"},
        #                 timeout=7.0,
        #             )
        #             if resp.status_code == 200:
        #                 js = resp.json() or []
        #                 if js:
        #                     return float(js[0]["lat"]), float(js[0]["lon"])
        #         except Exception:
        #             return None
        #         return None

        #     def _nearby_cities(lat: float, lon: float, radius_km: int = 40) -> List[str]:
        #         try:
        #             overpass = "https://overpass-api.de/api/interpreter"
        #             q = (
        #                 f"[out:json][timeout:15];("
        #                 f"node[\"place\"~\"city|town\"](around:{radius_km*1000},{lat},{lon});"
        #                 f");out tags 20;"
        #             )
        #             resp = requests.post(overpass, data={"data": q}, timeout=15.0)
        #             if resp.status_code == 200:
        #                 js = resp.json() or {}
        #                 out: List[str] = []
        #                 for el in js.get("elements", []):
        #                     name = (el.get("tags", {}).get("name") or "").strip()
        #                     if name:
        #                         out.append(name)
        #                 return out[:8]
        #         except Exception:
        #             return []
        #         return []
        #     if base_loc:
        #         out.append(base_loc)
        #         out.append(f"near {base_loc}")
        #         city_only = base_loc.split(',')[0].strip()
        #         if city_only and city_only.lower() != base_loc.lower():
        #             out.append(city_only)
        #             out.append(f"{city_only} area")
        #         parts = [p.strip() for p in base_loc.split(',')]
        #         if len(parts) >= 2:
        #             state_only = parts[-1]
        #             if state_only:
        #                 out.append(state_only)
        #         # dynamic nearby cities
        #         geo = _geocode(base_loc)
        #         if geo:
        #             lat, lon = geo
        #             for city in _nearby_cities(lat, lon):
        #                 out.append(city)
        #                 out.append(f"near {city}")
        #     seen = set()
        #     uniq: List[str] = []
        #     for v in out:
        #         k = v.lower()
        #         if k not in seen:
        #             uniq.append(v)
        #             seen.add(k)
        #     return uniq[:8]

        # if (limit == 0 or len(hrefs) < min(limit, 400)):
        #     q_variants = _query_variants(q)
        #     loc_variants = _location_variants(loc) or [""]
        #     for qi, li in product(q_variants, loc_variants):
        #         if 0 < limit <= len(hrefs):
        #             break
        #         qv = f"{qi} {li}".strip()
        #         if qv.lower() == q_full.lower():
        #             continue
        #         url = _add_locale_qs(f"https://www.google.com/maps/search/{quote_plus(qv)}")
        #         try:
        #             await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        #             await _click_consent_if_present(page)
        #         except Exception:
        #             continue
        #         print(f"{ts()} | DEBUG   | [gmaps] variant query='{qv}'")
        #         async def _progress_update_v(payload: Dict[str, int]) -> None:
        #             try:
        #                 p2 = dict(payload)
        #                 p2["query"] = qv
        #                 p2["hrefs_total"] = len(hrefs)
        #                 _append_meta(meta_path, gmaps_progress=p2, last_heartbeat=ts())
        #             except Exception:
        #                 pass
        #         more = await _collect_anchor_hrefs(page, limit, dmin, dmax, progress_cb=_progress_update_v)
        #         if more:
        #             hrefs = dedupe_hrefs(list(dict.fromkeys(hrefs + more)))
        #             print(f"{ts()} | INFO    | [gmaps] hrefs.accumulated={len(hrefs)}")

        metrics = Metrics()
        failures = []
        metrics.queued = len(hrefs)
        _append_meta(meta_path, counters={"queued": len(hrefs), "written": 0, "failures": 0})
        logger.info("[gmaps] hrefs.collected", count=len(hrefs))

        seen: Set[str] = set()
        seen_domains: Set[str] = set()
        sem = asyncio.Semaphore(concurrency)
        written = 0
        stop_event = asyncio.Event()

        async def guarded_process_with_wall(use_ctx: BrowserContext, h: str) -> Tuple[
            Optional[Dict[str, Any]], Optional[str]]:
            """
            Returns: (data, fail_reason)  where fail_reason is None on success.
            This function must NOT mutate outer 'failures'; the caller (worker) will.
            """
            try:
                data = await asyncio.wait_for(
                    _process_href(use_ctx, h, dmin, dmax),
                    timeout=HARD_WORKER_TIMEOUT
                )
                if data:
                    return data, None
                return None, "no_data"
            except asyncio.TimeoutError:
                return None, "hard_timeout"
            except Exception as e:
                return None, f"{type(e).__name__}:{e}"

        async def worker(h: str) -> None:
            nonlocal written
            if stop_event.is_set() or h in seen:
                return
            seen.add(h)
            if stop_event.is_set():
                return

            if ip_per_worker:
                idx = pool._rr % pool.size
                pool._rr += 1
                async with pool.locks[idx]:
                    use_ctx = pool.contexts[idx]
                    data, fail_reason = await with_sem(sem, guarded_process_with_wall(use_ctx, h))
            else:
                data, fail_reason = await with_sem(sem, guarded_process_with_wall(ctx, h))

            if not data:
                failures.append((h, fail_reason or "no_data"))
                return

            rec = {
                "source": "gmaps",
                "search_term": q,
                "search_location": loc,
                **data,
            }
            # Domain-level de-duplication: keep only one record per website domain
            domain = _normalize_domain(rec.get("website"))
            if domain:
                if domain in seen_domains:
                    print(f"{ts()} | INFO    | [gmaps] skip domain-duplicate: {domain} name={rec.get('name')!r}")
                    return
                seen_domains.add(domain)
            to_jsonl(outfile, rec)
            to_jsonl(os.path.join(run_dir, "gmaps.jsonl"), rec)
            written += 1
            _append_meta(meta_path,
                         counters={"queued": metrics.queued, "written": written, "failures": len(failures)})
            if 0 < limit <= written:
                stop_event.set()
                logger.info("[gmaps] limit.hit", written=written, limit=limit)

        tasks: List[asyncio.Task] = []
        task_info: Dict[asyncio.Task, Tuple[str, float]] = {}
        WALL_KILL = HARD_WORKER_TIMEOUT + 20.0

        for h in hrefs:
            t = asyncio.create_task(worker(h))
            tasks.append(t)
            task_info[t] = (h, time.time())

        if tasks:
            last_print = time.time()
            prev_done_count = 0
            while tasks:
                done, pending = await asyncio.wait(tasks, timeout=1.0, return_when=asyncio.FIRST_COMPLETED)

                done_count = len(done)
                if done_count > 0:
                    prev_done_count += done_count
                    for d in done:
                        task_info.pop(d, None)
                    tasks = list(pending)

                if time.time() - last_print > 5:
                    logger.info("[gmaps] heartbeat", written=written, pending=len(tasks), failures=len(failures))
                    _append_meta(meta_path,
                                 last_heartbeat=ts(),
                                 counters={"queued": metrics.queued, "written": written, "failures": len(failures)})
                    last_print = time.time()

                now = time.time()
                to_cancel: List[asyncio.Task] = []
                for t, (href_running, started_at) in list(task_info.items()):
                    if now - started_at > WALL_KILL:
                        to_cancel.append(t)
                        failures.append((href_running, "watchdog_cancel"))

                for t in to_cancel:
                    with contextlib.suppress(Exception):
                        t.cancel()
                    task_info.pop(t, None)

                if to_cancel:
                    tasks = [t for t in tasks if t not in to_cancel]

        logger.info("[gmaps] pass.complete", pass_no=1, written=written, failures=len(failures))
        if failures:
            reason_counts = {}
            for _, r in failures:
                reason_counts[r] = reason_counts.get(r, 0) + 1
            logger.info("[gmaps] pass.stats", failures=len(failures),
                        reasons=sorted(reason_counts.items(), key=lambda x: -x[1])[:5])

        max_retries = int(getattr(args, "max_retries", 2) or 2)
        backoff = float(getattr(args, "retry_backoff", 1.6) or 1.6)
        _append_meta(meta_path, status="running", phase="retrying", last_heartbeat=ts())
        retry_no = 0
        while (limit == 0 or written < limit) and failures and retry_no < max_retries:
            retry_no += 1
            hrefs_to_retry = [h for (h, _) in failures]
            failures = []
            logger.warn("[gmaps] retry.start", pass_no=retry_no, candidates=len(hrefs_to_retry))

            delay = (backoff ** (retry_no - 1)) * max(dmin, 0.1)

            async def retry_worker(h: str) -> None:
                nonlocal written
                if stop_event.is_set():
                    return
                await asyncio.sleep(random.uniform(0, delay))
                if stop_event.is_set():
                    return

                if ip_per_worker:
                    idx = pool._rr % pool.size
                    pool._rr += 1
                    async with pool.locks[idx]:
                        use_ctx = pool.contexts[idx]
                        data, fail_reason = await with_sem(sem, guarded_process_with_wall(use_ctx, h))
                else:
                    data, fail_reason = await with_sem(sem, guarded_process_with_wall(ctx, h))

                if not data:
                    failures.append((h, fail_reason or "no_data"))
                    return

                rec = {
                    "source": "gmaps",
                    "search_term": q,
                    "search_location": loc,
                    **data,
                }
                # Domain-level de-duplication on retries as well
                domain = _normalize_domain(rec.get("website"))
                if domain:
                    if domain in seen_domains:
                        print(f"{ts()} | INFO    | [gmaps] skip domain-duplicate(retry): {domain} name={rec.get('name')!r}")
                        return
                    seen_domains.add(domain)
                to_jsonl(outfile, rec)
                to_jsonl(os.path.join(run_dir, "gmaps.jsonl"), rec)

                written += 1
                logger.info("[gmaps] retry.success", href=h)
                _append_meta(meta_path,
                             counters={"queued": metrics.queued, "written": written, "failures": len(failures)},
                             last_heartbeat=ts())
                if 0 < limit <= written:
                    stop_event.set()

            tasks = [asyncio.create_task(retry_worker(h)) for h in hrefs_to_retry]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            logger.info("[gmaps] retry.complete", pass_no=retry_no,
                        written=written, still_failed=len(failures))

        if failures:
            fail_path = os.path.join(OUT_ROOT, f"failures_{run_id}.jsonl")
            with open(fail_path, "a", encoding="utf-8") as f:
                for h, reason in failures:
                    f.write(json.dumps({"href": h, "reason": reason}) + "\n")
            logger.warn("[gmaps] failures.persisted", count=len(failures), path=fail_path)

        logger.info("[gmaps] job.done", written=written, outfile=outfile)
        logger.close()

        final_status = "running"
        phase_value = "maps_scraped" if written > 0 else "maps_scraping_failed"

        _append_meta(
            meta_path,
            status=final_status,
            phase=phase_value,
            counters={"queued": metrics.queued, "written": written, "failures": len(failures)}
        )

        export_csv = int(getattr(args, "export_csv", 1) or 1)
        if export_csv:
            gmaps_jsonl = os.path.join(run_dir, "gmaps.jsonl")
            if os.path.exists(gmaps_jsonl):
                gmaps_csv = os.path.join(run_dir, "gmaps.csv")
                try:
                    _jsonl_to_csv(gmaps_jsonl, gmaps_csv)
                    print(f"{ts()} | INFO    | [gmaps] CSV exported → {gmaps_csv}")
                except Exception as e:
                    print(f"{ts()} | WARNING | [gmaps] CSV export failed: {e}")

        copy_to_run_dir = int(getattr(args, "copy_to_run_dir", 1) or 1)
        if copy_to_run_dir:
            try:
                run_gmaps_jsonl = os.path.join(run_dir, "gmaps.jsonl")
                if not os.path.exists(run_gmaps_jsonl) and os.path.exists(outfile):
                    shutil.copyfile(outfile, run_gmaps_jsonl)
            except Exception:
                pass
        if ip_per_worker:
            await pool.close()
        else:
            with contextlib.suppress(Exception):
                await ctx.close()
            with contextlib.suppress(Exception):
                await browser.close()
