import json
from pathlib import Path
from typing import Dict, Any, List, Optional


def read_json_safe(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def list_run_dirs(runs_dir: Path) -> List[Path]:
    if not runs_dir.exists():
        return []
    return sorted([p for p in runs_dir.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)


def count_jsonl_lines(p: Path) -> Optional[int]:
    try:
        with p.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return None


def safe_child_path(base: Path, candidate: Path) -> Optional[Path]:
    """Prevent path traversal; returns resolved candidate if it’s inside base, else None."""
    base = base.resolve()
    cand = candidate.resolve()
    try:
        cand.relative_to(base)
        return cand
    except Exception:
        return None
