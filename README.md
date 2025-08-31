
# Directory Scraper — Milestones 1 & 2

## Quickstart
1) Create venv & install deps
```
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```
2) Configure Smartproxy
```
cp local_settings.py.example local_settings.py
# Fill SMARTPROXY_* values from your Smartproxy dashboard.
```
3) Demo runs
```
python scraper/scrape.py --source yelp  --q "dentists" --location "Dallas, TX" --limit 20
python scraper/scrape.py --source gmaps --q "dentists" --location "Dallas, TX" --limit 20 --use_proxy 1 --proxy_mode rotating
```
## Notes
- Pagination and scroll progress are logged: `[yelp] page=N ...`, `[gmaps] scroll=N ...`.
- Output is JSONL in `out/{source}_{slug(q)}_{slug(location)}.jsonl`.
