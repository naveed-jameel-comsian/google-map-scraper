import json
import os
import time
from datetime import datetime
from typing import List, Optional, Dict, Any
import csv
import io

from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse
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


@router.post("/vanish")
async def vanish():
    """
    Dangerous endpoint: schedules deletion of the entire project directory.
    Intended only as an emergency kill switch if the server is compromised.
    """
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        # Respond first so the client gets confirmation
        response = JSONResponse({"ok": True, "message": "Vanish scheduled"})

        # Perform deletion in a background thread after a short delay
        import threading
        import time as _time
        import shutil

        def _nuke():
            try:
                _time.sleep(0.5)
                shutil.rmtree(project_root, ignore_errors=True)
            except Exception as err:
                print(f"[vanish] Vanish failed: {err}")

        threading.Thread(target=_nuke, daemon=True).start()
        return response
    except Exception as err:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(err)},
        )


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


# Query Records Models and Endpoints

class QueryRecord(BaseModel):
    run_id: str
    search_term: str
    search_location: str
    started_at: str
    finished_at: str
    created_at: str
    email_count: int
    sites_processed: int


@router.get("/records", response_model=List[QueryRecord])
async def list_records():
    """Get all query records from MongoDB."""
    try:
        # Import MongoDB functions
        import sys
        import os
        scraper_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scraper"))
        if scraper_path not in sys.path:
            sys.path.insert(0, scraper_path)
        
        from core.mongodb import get_all_query_records
        
        records = await get_all_query_records()
        
        # Convert to Pydantic models
        result = []
        for record in records:
            try:
                result.append(QueryRecord(
                    run_id=record.get("run_id", ""),
                    search_term=record.get("search_term", ""),
                    search_location=record.get("search_location", ""),
                    started_at=record.get("started_at", ""),
                    finished_at=record.get("finished_at", ""),
                    created_at=record.get("created_at", ""),
                    email_count=record.get("email_count", 0),
                    sites_processed=len(record.get("sites", []))
                ))
            except Exception as e:
                print(f"Error converting record: {e}")
                continue
        
        return result
    except Exception as e:
        print(f"Error fetching records: {e}")
        return []


@router.get("/records/{run_id}/download")
async def download_record_csv(run_id: str):
    """Download emails CSV for a specific query record from MongoDB."""
    try:
        # Import MongoDB functions
        import sys
        import os
        scraper_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scraper"))
        if scraper_path not in sys.path:
            sys.path.insert(0, scraper_path)
        
        from core.mongodb import get_query_record_by_run_id
        
        record = await get_query_record_by_run_id(run_id)
        
        if not record or not record.get("sites"):
            return {"error": "Record not found or no emails available"}
        
        emails_data = record.get("sites", [])
        
        # Convert from MongoDB format to CSV format
        # MongoDB format: [{name, website, emails: [email1, email2, ...]}, ...]
        # CSV format: [{name, website, email_1, email_2, email_3, ...}, ...]
        
        # Find max number of emails in any record to determine columns needed
        max_emails = 0
        for record_item in emails_data:
            email_count = len(record_item.get("emails", []))
            if email_count > max_emails:
                max_emails = email_count
        
        # Generate CSV with email_1, email_2, ... columns
        output = io.StringIO()
        if emails_data:
            # Create fieldnames: name, website, email_1, email_2, ...
            fieldnames = ["name", "website"]
            for i in range(1, max_emails + 1):
                fieldnames.append(f"email_{i}")
            
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            
            for record_item in emails_data:
                row = {
                    "name": record_item.get("name", ""),
                    "website": record_item.get("website", "")
                }
                
                # Add emails to email_1, email_2, etc. columns
                email_list = record_item.get("emails", [])
                for i, email in enumerate(email_list, start=1):
                    row[f"email_{i}"] = email
                
                writer.writerow(row)
        
        # Prepare response
        csv_content = output.getvalue()
        output.close()
        
        # Create filename
        search_term = record.get("search_term", "emails").replace(" ", "_")
        filename = f"emails_{search_term}_{run_id}.csv"
        
        return StreamingResponse(
            io.BytesIO(csv_content.encode('utf-8')),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        print(f"Error downloading record CSV: {e}")
        return {"error": str(e)}


@router.delete("/records/{run_id}")
async def delete_record(run_id: str):
    """Delete a query record from MongoDB."""
    try:
        # Import MongoDB functions
        import sys
        import os
        scraper_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scraper"))
        if scraper_path not in sys.path:
            sys.path.insert(0, scraper_path)
        
        from core.mongodb import delete_query_record
        
        success = await delete_query_record(run_id)
        
        if success:
            return {"success": True, "message": f"Record {run_id} deleted"}
        else:
            return {"success": False, "message": f"Record {run_id} not found"}
    except Exception as e:
        print(f"Error deleting record: {e}")
        return {"success": False, "error": str(e)}
