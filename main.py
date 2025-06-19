import os
import asyncio
import random
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from fastpoll.impl.fastpoll_mongo import FastPollMongo
from fastpoll.fastpoll_models import FPJobStatus, ReportProgressFuncType

import httpx
from bs4 import BeautifulSoup

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "jobsdb")
COLLECTION_NAME = "jobs"


# Create a global instance of FastPollMongo
fast_poll: Optional[FastPollMongo] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize FastPollMongo during app startup
    global fast_poll
    fast_poll = await FastPollMongo.create(mongo_uri=MONGO_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME)
    yield


app = FastAPI(lifespan=lifespan)


class ScrapeJobPayload(BaseModel):
    url: str = Field(
        ...,
        description="The URL of the web page to scrape.",
        example="https://example.com",
    )


@app.post("/jobs", response_model=FPJobStatus, status_code=201)
async def create_job(job: ScrapeJobPayload, background_tasks: BackgroundTasks):
    job_id = await fast_poll.run_job(
        func=process_test_very_heavy_long_job,
        url=job.url,
    )
    return await fast_poll.get_job_status(job_id)


@app.get("/jobs/{job_id}", response_model=FPJobStatus)
async def get_job(job_id: str):
    try:
        return await fast_poll.get_job_status(job_id)
    except ValueError:
        raise HTTPException(404, "Job not found")


# ---- Processing logic ----


async def process(payload: dict, report_progress: ReportProgressFuncType) -> dict:
    """
    Simulate heavy processing with progress reporting and random errors.
    The report_progress parameter is injected by FastPoll.
    """
    # Simulate heavy processing in 5 steps
    total_steps = 5

    for step in range(1, total_steps + 1):
        # Calculate progress percentage
        progress = int((step / total_steps) * 100)

        # Report progress if the callback is available
        if report_progress:
            await report_progress(progress, f"Processing step {step} of {total_steps}")

        # Simulate random error with 20% chance
        if random.random() < 0.2:
            raise RuntimeError(f"Random error occurred at step {step}")

        # Simulate work
        await asyncio.sleep(10)

    return {"processed": payload}


async def process_test_very_heavy_long_job(url: str, report_progress: ReportProgressFuncType) -> dict:
    """
    Scrape a web page, follow all found links, perform real network and parsing work, and report progress over at least 20 minutes.
    """
    import hashlib
    import math

    if not url:
        raise ValueError("Payload must contain a 'url' key.")

    visited = set()
    all_links = set()
    http_links = set()
    step = 0
    max_depth = 5  # How many levels of links to follow
    max_links_per_page = 10  # Limit to avoid infinite crawling
    max_total_links = 500  # Absolute max to avoid abuse
    queue = [(url, 0)]
    start_time = asyncio.get_event_loop().time()
    fetch_count = 0

    async with httpx.AsyncClient(verify=False, timeout=30) as client:
        while queue and fetch_count < max_total_links:
            current_url, depth = queue.pop(0)
            if current_url in visited or depth > max_depth:
                continue
            visited.add(current_url)
            step += 1
            progress = min(99, int((step / max_total_links) * 100))
            if report_progress:
                await report_progress(progress, f"Fetching ({step}/{max_total_links}) {current_url} (depth {depth})")
            try:
                response = await client.get(current_url)
                response.raise_for_status()
                html = response.text
            except Exception as e:
                if report_progress:
                    await report_progress(progress, f"Failed to fetch {current_url}: {e}")
                continue
            soup = BeautifulSoup(html, "html.parser")
            links = [a["href"] for a in soup.find_all("a", href=True)]
            # Normalize and filter links
            norm_links = []
            for l in links:
                if l.startswith("/"):
                    from urllib.parse import urljoin
                    l = urljoin(current_url, l)
                if l.startswith("http"):
                    norm_links.append(l)
            # Add to sets
            all_links.update(norm_links)
            http_links.update([l for l in norm_links if l.startswith("http")])
            # Enqueue next level
            for l in norm_links[:max_links_per_page]:
                if l not in visited and len(queue) < max_total_links:
                    queue.append((l, depth + 1))
            fetch_count += 1
            # Simulate heavy CPU work after each fetch
            dummy = 0
            for i in range(10**5, 10**5 + 20000):
                dummy += int(hashlib.sha256(str(i).encode()).hexdigest(), 16) % 1000
                dummy += int(math.sqrt(i))
            # Pace the crawl to avoid hammering servers and ensure long runtime
            await asyncio.sleep(10)
            # If running too fast, add more sleep
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed < (step * 12):
                await asyncio.sleep((step * 12) - elapsed)

    if report_progress:
        await report_progress(100, f"Crawling complete. Visited {len(visited)} pages, found {len(all_links)} links.")
    return {
        "url": url,
        "visited_pages": len(visited),
        "total_links_found": len(all_links),
        "http_links": list(http_links)[:10],
        "http_link_count": len(http_links),
        "note": f"Job followed links up to {max_depth} levels, max {max_total_links} pages, with real network and CPU work."
    }
