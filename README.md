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
```

### 3. Run a scrape

Scrape businesses from Google Maps:

```
python scraper/gmaps.py --q "dentists" --location "Dallas, TX" --limit 20 --use_proxy 1 --proxy_mode rotating
python scrape_verify_only.py --infile out/gmaps_dentists_dallas_tx.jsonl --run-id <RUN_ID>
```

### 4. Outputs for each run are stored under:

```
out/runs/<RUN_ID>/
├── gmaps.jsonl   # raw scraped businesses
├── gmaps.csv     # CSV export of businesses
├── emails.csv    # verified emails
├── meta.json     # run metadata
└── dashboard.log # structured logs
```

Note: After CSV export, the intermediate JSONL files are deleted to save storage.

### 5. Start the FastAPI dashboard:
```
uvicorn dashboard.server.main:app --reload --port 8000
```