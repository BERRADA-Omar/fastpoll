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
    Actually scrape a web page and perform some transformation with progress reporting.
    The report_progress parameter is injected by FastPoll.
    Expects payload to contain a 'url' key.
    """
    if not url:
        raise ValueError("Payload must contain a 'url' key.")

    if report_progress:
        await report_progress(10, f"Fetching URL: {url}")
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(url)
        response.raise_for_status()
        html = response.text

    if report_progress:
        await report_progress(40, "Parsing HTML content")
    soup = BeautifulSoup(html, "html.parser")
    # Example: extract all links
    links = [a["href"] for a in soup.find_all("a", href=True)]

    if report_progress:
        await report_progress(70, f"Found {len(links)} links. Performing transformation.")
    # Example transformation: filter only http(s) links and count them
    http_links = [link for link in links if link.startswith("http")]
    link_count = len(http_links)

    if report_progress:
        await report_progress(100, "Scraping and transformation complete.")
    return {
        "url": url,
        "total_links": len(links),
        "http_links": http_links[:10],  # return only first 10 for brevity
        "http_link_count": link_count,
    }
