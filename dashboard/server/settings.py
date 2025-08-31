from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]

RUNS_DIR = ROOT_DIR / "out" / "runs"

SCRAPER_SCRIPT = ROOT_DIR / "scraper" / "scrape.py"

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
