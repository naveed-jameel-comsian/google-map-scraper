from typing import List, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .scrape_verify_only import scrape_site, MAX_PAGES, INCLUDE_EXTERNAL


app = FastAPI(title="Scraper API", version="1.0.0")


class ScrapeRequest(BaseModel):
    start_url: str
    max_pages: Optional[int] = None
    include_external: Optional[bool] = None


@app.post("/scrape-site")
async def scrape_site_post(payload: ScrapeRequest):
    """
    Trigger email scraping for a given site.
    """
    max_pages = payload.max_pages or MAX_PAGES
    include_external = (
        INCLUDE_EXTERNAL if payload.include_external is None else payload.include_external
    )

    try:
        results: List[Dict[str, str]] = await scrape_site(
            payload.start_url,
            max_pages=max_pages,
            include_external=include_external,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "start_url": payload.start_url,
        "max_pages": max_pages,
        "include_external": include_external,
        "count": len(results),
        "results": results,
    }


@app.get("/scrape-site")
async def scrape_site_get(
    start_url: str,
    max_pages: int = MAX_PAGES,
    include_external: bool = INCLUDE_EXTERNAL,
):
    """
    Convenience GET endpoint mirroring the POST body.
    """
    try:
        results: List[Dict[str, str]] = await scrape_site(
            start_url,
            max_pages=max_pages,
            include_external=include_external,
            
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "start_url": start_url,
        "max_pages": max_pages,
        "include_external": include_external,
        "count": len(results),
        "results": results,
    }

