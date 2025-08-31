import json
import os
import time
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import APIRouter
from pydantic import BaseModel

from dashboard.server.settings import RUNS_DIR

router = APIRouter()

HEARTBEAT_GRACE_SECONDS = int(os.getenv("HEARTBEAT_GRACE_SECONDS", "150"))  # ~2.5 min


class RunSummary(BaseModel):
    run_id: str
    source: Optional[str] = None
    search_term: Optional[str] = None
    search_location: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status: Optional[str] = None
    status_derived: str
    counters: Dict[str, Any] = {}
    email_counters: Dict[str, Any] = {}
    files: Dict[str, str] = {}
    last_heartbeat: Optional[str] = None


def _safe_load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


@router.get("/_whoami")
def whoami():
    print(f"[dashboard] api.py loaded from {__file__}")
    return {"file": __file__, "mtime": os.path.getmtime(__file__)}


def _iso_to_epoch(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return None


def _derive_status(meta: Dict[str, Any], run_dir: str) -> str:
    raw = (meta.get("status") or "").lower().strip()
    if raw in ("done", "failed"):
        return raw or "unknown"

    last_hb_str = meta.get("last_heartbeat")
    last_hb_ts = _iso_to_epoch(last_hb_str) if last_hb_str else None

    meta_path = os.path.join(run_dir, "meta.json")
    candidates = [meta_path]
    for name in ("gmaps.jsonl", "emails.jsonl", "gmaps.csv", "emails.csv"):
        p = os.path.join(run_dir, name)
        if os.path.exists(p):
            candidates.append(p)

    freshest_mtime = 0.0
    for p in candidates:
        try:
            m = os.path.getmtime(p)
            freshest_mtime = max(freshest_mtime, m)
        except Exception:
            pass

    now = time.time()
    last_activity = last_hb_ts or (freshest_mtime or 0)

    if raw == "running":
        if last_activity and (now - last_activity) <= HEARTBEAT_GRACE_SECONDS:
            return "running"
        else:
            return "stalled"

    if meta.get("finished_at"):
        return "done"

    return raw or "unknown"


def _started_ts(meta: Dict[str, Any], run_dir: str) -> float:
    """
    Return a numeric 'started' timestamp for sorting, with robust fallbacks:
    1) meta['started_at'] parsed as "%Y-%m-%d %H:%M:%S"
    2) mtime of meta.json
    3) 0.0
    """
    ts = _iso_to_epoch(meta.get("started_at"))
    if ts:
        return ts
    try:
        return os.path.getmtime(os.path.join(run_dir, "meta.json"))
    except Exception:
        return 0.0


@router.get("/runs", response_model=List[RunSummary])
def list_runs():
    if not os.path.isdir(RUNS_DIR):
        return []

    runs: List[RunSummary] = []
    for run_id in os.listdir(RUNS_DIR):
        run_dir = os.path.join(RUNS_DIR, run_id)
        if not os.path.isdir(run_dir):
            continue
        meta_path = os.path.join(run_dir, "meta.json")
        meta = _safe_load_json(meta_path)

        status_derived = _derive_status(meta, run_dir)

        runs.append(RunSummary(
            run_id=run_id,
            source=meta.get("source"),
            search_term=meta.get("search_term"),
            search_location=meta.get("search_location"),
            started_at=meta.get("started_at"),
            finished_at=meta.get("finished_at"),
            status=meta.get("status"),
            status_derived=status_derived,
            counters=meta.get("counters") or {},
            email_counters=meta.get("email_counters") or {},
            files=meta.get("files") or {},
            last_heartbeat=meta.get("last_heartbeat"),
        ))

    runs.sort(
        key=lambda r: (
                _iso_to_epoch(r.started_at) or
                (os.path.getmtime(os.path.join(RUNS_DIR, r.run_id, "meta.json"))
                 if os.path.exists(os.path.join(RUNS_DIR, r.run_id, "meta.json")) else 0.0)
        ),
        reverse=True
    )
    return runs
