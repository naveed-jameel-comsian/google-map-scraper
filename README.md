# Directory Scraper

A scraping + email verification pipeline with a FastAPI dashboard.  
Currently supports **Google Maps** as the source for businesses, then scrapes and verifies emails from their websites.

---

## Quickstart

### 1. Setup environment
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

### 2. Copy the example config and set your credentials:
```
cp local_settings.py.example local_settings.py

Fill in:
	•	SMARTPROXY_* (if using Smartproxy for Maps scraping)
	•	HUNTER_API_KEY (for email verification via Hunter)
	•	OUT_ROOT (optional; default `out`)
```

### 3. End-to-end run (scrape Maps → crawl sites → verify → CSV)

Scrape businesses from Google Maps and then verify/csv automatically:

```
python scraper/scrape.py --source gmaps --q "dentists" --location "Dallas, TX" --limit 200 --use_proxy 1 --proxy_mode rotating --concurrency 8 --verify_concurrency 6 --run_id demo_run

python scraper/scrape.py --source gmaps --q "dentists" --location "Dallas, TX" --limit 50 --use_proxy 0 --concurrency 6 --verify_concurrency 6 --run_id demo_no_proxy

python scraper/scrape.py --source gmaps --q "dentists" --location "Dallas, TX" --limit 5 --use_proxy 1 --proxy_mode sticky --run_id fixed_proxy_test

python scraper/scrape.py --source gmaps --q "dentists" --location "Dallas, TX" --limit 500 --use_proxy 1 --proxy_mode sticky --concurrency 12 --verify_concurrency 10 --run_id production_with_proxy
```

Outputs are placed under `OUT_ROOT` (default `./out`). A run folder will be created at `out/runs/<RUN_ID>/` with:

```
out/runs/<RUN_ID>/
├── gmaps.jsonl
├── gmaps.csv
├── emails.jsonl
├── emails.csv
├── unique_emails.csv   # flat, deduplicated verified emails
├── dashboard.log
└── meta.json
```

Notes:
- `unique_emails.csv` contains deduplicated Hunter-verified emails for proof-of-throughput.
- Intermediate JSONLs may be deleted after CSV export to save space.

Note: After CSV export, the intermediate JSONL files are deleted to save storage.

### 4. Start the FastAPI dashboard:
```
uvicorn dashboard.server.main:app --reload --port 8000
```

From the dashboard you can launch scrapes and download CSVs/logs per run.

### 5. Proof/logging

- Live logs are written to `out/runs/<RUN_ID>/dashboard.log`.
- Machine logs are written as JSONL to `out/<RUN_ID>.log.jsonl`.
- Run metadata/heartbeats in `out/runs/<RUN_ID>/meta.json` track progress and counts.

### 6. Performance target

For ≥1,000 verified emails/hour, tune:
- Increase `--limit`, `--concurrency`, and `--site-concurrency` (verifier) as network allows.
- Ensure `SMARTPROXY_*` credentials are valid and consider `ip_per_worker=1` for more parallelism.
- Use `--verify_concurrency` 8–16 with a paid Hunter plan.