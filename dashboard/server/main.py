from pathlib import Path
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .settings import RUNS_DIR, STATIC_DIR, TEMPLATES_DIR
from .tasks import launch_scrape
from .utils import read_json_safe, list_run_dirs, count_jsonl_lines, safe_child_path
import os
import signal
import shutil

app = FastAPI(title="Scraper Dashboard")

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

import os, time
from datetime import datetime

HEARTBEAT_GRACE_SECONDS = int(os.getenv("HEARTBEAT_GRACE_SECONDS", "150"))


def _iso_to_epoch(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return None


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/fragment/runs", response_class=HTMLResponse)
def fragment_runs(request: Request):
    runs = _collect_runs()
    return templates.TemplateResponse("_runs_table.html", {"request": request, "runs": runs})


@app.get("/fragment/running", response_class=HTMLResponse)
def fragment_running(request: Request):
    runs = _collect_runs()
    running = [r for r in runs if r.get("status") == "running"]
    running.sort(key=lambda r: r.get("started_at_num", 0.0), reverse=True)
    return templates.TemplateResponse("_running_table.html", {"request": request, "runs": running})


@app.post("/launch", response_class=HTMLResponse)
def launch(
        request: Request,
        source: str = Form(...),
        q: str = Form(...),
        location: str = Form(...),
        use_proxy: int = Form(0),
        concurrency: int = Form(8),
        ip_per_worker: int = Form(0),
        delay_min: float = Form(0.05),
        delay_max: float = Form(0.15),
        max_retries: int = Form(2),
        retry_backoff: float = Form(1.6),
        verify_concurrency: int = Form(3),
        max_pages: int = Form(20),
        use_hunter_cache: int = Form(1),
        proxy_mode: str = Form("rotating"),
        run_id: str = Form(""),
):
    launch_info = launch_scrape(
        source=source,
        q=q,
        location=location,
        use_proxy=use_proxy,
        concurrency=concurrency,
        ip_per_worker=ip_per_worker,
        delay_min=delay_min,
        delay_max=delay_max,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        verify_concurrency=verify_concurrency,
        max_pages=max_pages,
        use_hunter_cache=use_hunter_cache,
        proxy_mode=proxy_mode,
        run_id=run_id or None,
    )
    html = f"""
    <div id="toast-slot">
      <div class="toast success">
        Launched run <strong>{launch_info["run_id"]}</strong> (PID {launch_info["pid"]}).
      </div>
    </div>
    """
    return HTMLResponse(content=html)


@app.get("/download/{run_id}/{filename}")
def download_file(run_id: str, filename: str):
    run_dir = RUNS_DIR / run_id
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail="run not found")

    file_path = safe_child_path(run_dir, run_dir / filename)
    if not file_path or not file_path.exists():
        raise HTTPException(status_code=404, detail="file not found")

    media_type = "text/csv" if file_path.suffix.lower() == ".csv" else "application/octet-stream"
    return FileResponse(
        path=str(file_path),
        media_type=media_type,
        filename=f"{run_id}-{file_path.name}",
    )


@app.get("/log_live/{run_id}", response_class=HTMLResponse)
def log_live_page(run_id: str, request: Request):
    run_dir = RUNS_DIR / run_id
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail="run not found")

    meta = read_json_safe(run_dir / "meta.json")

    bits = []
    term = (meta.get("search_term") or "").strip()
    loc = (meta.get("search_location") or "").strip()
    if term:
        bits.append(term)
    if loc:
        bits.append(loc)
    bits.append(run_id)
    title = " — ".join(bits)

    return templates.TemplateResponse(
        "log_live.html",
        {
            "request": request,
            "run_id": run_id,
            "title": title,
            "search_term": term,
            "search_location": loc,
        },
    )


@app.get("/log_view/{run_id}", response_class=HTMLResponse)
def log_view_page(run_id: str, request: Request):
    run_dir = RUNS_DIR / run_id
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail="run not found")

    meta = read_json_safe(run_dir / "meta.json")
    term = (meta.get("search_term") or "").strip()
    loc = (meta.get("search_location") or "").strip()
    bits = []
    if term: bits.append(term)
    if loc:  bits.append(loc)
    bits.append(run_id)
    title = " — ".join(bits)

    log_path = run_dir / "dashboard.log"
    sp = safe_child_path(run_dir, log_path)
    if not sp or not sp.exists():
        return templates.TemplateResponse(
            "log_static.html",
            {
                "request": request,
                "run_id": run_id,
                "title": title,
                "log_text": "(no dashboard.log found for this run)",
                "has_log": False,
                "download_href": None,
            },
        )

    return templates.TemplateResponse(
        "log_static.html",
        {
            "request": request,
            "run_id": run_id,
            "title": title,
            "log_text": sp.read_text(encoding="utf-8", errors="replace"),
            "has_log": True,
            "download_href": f"/download/{run_id}/dashboard.log",
        },
    )


def _classify_csv(meta: Dict[str, Any], p: Path) -> Optional[str]:
    """
    Return 'gmaps' or 'emails' for a CSV filename, or None if unknown.
    """
    lname = p.name.lower()
    if "email" in lname:
        return "emails"
    if "gmaps" in lname or "map" in lname:
        return "gmaps"
    if lname == "emails.csv":
        return "emails"
    if lname == "gmaps.csv":
        return "gmaps"
    if lname == "output.csv":
        src = (meta.get("source") or "").lower()
        if src == "gmaps":
            return "gmaps"
        if src == "emails":
            return "emails"
    return None


def _collect_runs() -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    for d in list_run_dirs(RUNS_DIR):
        meta = read_json_safe(d / "meta.json")
        counters = meta.get("counters") or {}
        email_counters = meta.get("email_counters") or {}

        files = []
        gmaps_csv = None
        emails_csv = None

        for p in d.glob("*"):
            if not p.is_file():
                continue
            files.append(p.name)
            if p.suffix.lower() == ".csv":
                lname = p.name.lower()
                if ("email" in lname or "emails" in lname) and emails_csv is None:
                    emails_csv = p.name
                elif ("gmaps" in lname or "map" in lname) and gmaps_csv is None:
                    gmaps_csv = p.name
                elif p.name == "emails.csv" and emails_csv is None:
                    emails_csv = p.name
                elif p.name == "gmaps.csv" and gmaps_csv is None:
                    gmaps_csv = p.name
                elif p.name == "output.csv":
                    src = (meta.get("source") or "").lower()
                    if src == "gmaps" and gmaps_csv is None:
                        gmaps_csv = p.name
                    elif src == "emails" and emails_csv is None:
                        emails_csv = p.name

        has_gmaps_csv = gmaps_csv is not None
        has_emails_csv = emails_csv is not None
        has_log = (d / "dashboard.log").exists()

        written = counters.get("written")
        if written is None:
            gmaps_jsonl = d / "gmaps.jsonl"
            written = count_jsonl_lines(gmaps_jsonl) or 0

        raw_status = (meta.get("status") or "").lower().strip()
        finished = bool(meta.get("finished_at"))

        last_hb = meta.get("last_heartbeat")
        last_ts = _iso_to_epoch(last_hb) if last_hb else None
        if not last_ts:
            try:
                last_ts = (d / "meta.json").stat().st_mtime
            except Exception:
                last_ts = None

        now = time.time()
        fresh = (last_ts is not None) and ((now - last_ts) <= HEARTBEAT_GRACE_SECONDS)

        if not finished and (raw_status in ("running", "scraping maps", "scraping_emails", "scraping emails",
                                            "maps_scraped")) and fresh:
            ui_status = "running"
        elif finished:
            ui_status = "done"
        else:
            ui_status = "running" if fresh else (raw_status or "unknown")

        sites_processed = (email_counters.get("sites_processed") or 0)
        emails_verified = (email_counters.get("emails_verified") or 0)

        started_num = _iso_to_epoch(meta.get("started_at"))
        if not started_num:
            try:
                started_num = (d / "meta.json").stat().st_mtime
            except Exception:
                started_num = 0.0

        runs.append({
            "run_id": meta.get("run_id") or d.name,
            "status": ui_status,
            "phase": meta.get("phase"),
            "started_at": meta.get("started_at"),
            "started_at_num": started_num,
            "finished_at": meta.get("finished_at"),
            "source": meta.get("source"),
            "search_term": meta.get("search_term"),
            "search_location": meta.get("search_location"),
            "queued": counters.get("queued"),
            "written": written,
            "failures": counters.get("failures"),
            "gmaps_progress": meta.get("gmaps_progress") or {},
            'sites_processed': sites_processed,
            'emails_verified': emails_verified,
            "files": files,
            "gmaps_csv": gmaps_csv,
            "emails_csv": emails_csv,
            "has_gmaps_csv": has_gmaps_csv,
            "has_emails_csv": has_emails_csv,
            "has_log": has_log,
        })

    runs.sort(key=lambda r: r.get("started_at_num", 0.0), reverse=True)
    return runs
