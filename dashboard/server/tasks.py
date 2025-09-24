import subprocess
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional

from .settings import SCRAPER_SCRIPT, RUNS_DIR
import os


def new_run_id(prefix: Optional[str] = None) -> str:
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{stamp}" if prefix else stamp


def launch_scrape(
        source: str,
        q: str,
        location: str,
        limit: int = 0,
        use_proxy: int = 0,
        concurrency: int = 8,
        ip_per_worker: int = 0,
        delay_min: float = 0.05,
        delay_max: float = 0.15,
        max_retries: int = 2,
        retry_backoff: float = 1.6,
        verify_concurrency: int = 3,
        max_pages: int = 20,
        use_hunter_cache: int = 1,
        proxy_mode: str = "rotating",
        run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Start scraper/scrape.py as a detached subprocess.
    Writes its stdout/stderr to out/runs/<run_id>/dashboard.log
    """
    if not run_id:
        run_id = new_run_id(prefix=f"{source}")

    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "dashboard.log"

    cmd: List[str] = [
        sys.executable, str(SCRAPER_SCRIPT),
        "--source", source,
        "--q", q,
        "--location", location,
        "--limit", str(limit),
        "--use_proxy", str(use_proxy),
        "--proxy_mode", str(proxy_mode),
        "--concurrency", str(concurrency),
        "--ip_per_worker", str(ip_per_worker),
        "--delay_min", str(delay_min),
        "--delay_max", str(delay_max),
        "--max_retries", str(max_retries),
        "--retry_backoff", str(retry_backoff),
        "--verify_concurrency", str(verify_concurrency),
        "--max_pages", str(max_pages),
        "--use_hunter_cache", str(use_hunter_cache),
        "--run_id", run_id,
        "--export_csv", "1",
    ]

    log_f = open(log_path, "a", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(SCRAPER_SCRIPT.parent.parent),
        start_new_session=True,
    )

    # Persist PID for later stop/delete actions
    try:
        (run_dir / "pid").write_text(str(proc.pid), encoding="utf-8")
        (run_dir / "cmd.txt").write_text(" ".join(cmd), encoding="utf-8")
    except Exception:
        pass

    return {
        "run_id": run_id,
        "pid": proc.pid,
        "cmd": cmd,
        "log_path": str(log_path),
    }
