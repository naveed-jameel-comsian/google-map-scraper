from pathlib import Path
import os

OUT_ROOT = Path(os.getenv("OUT_ROOT", "out")).resolve()

RUNS_DIR = Path(os.getenv("RUN_REGISTRY_DIR", OUT_ROOT / "runs")).resolve()

SCRAPER_SCRIPT = Path(os.getenv("SCRAPER_SCRIPT", Path(__file__).resolve().parents[2] / "scraper" / "scrape.py"))

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
