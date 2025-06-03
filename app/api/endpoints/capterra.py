import asyncio
import sys
import traceback
from datetime import datetime
from typing import Dict, Any, List # Added List

from fastapi import APIRouter, HTTPException, Body, Depends # Added Depends if needed later
from pydantic import HttpUrl # Import HttpUrl for type validation in loop

from app.schema.capterra_models import ScrapeRequest
from app.service.capterra_service import scrape_capterra_async
from app.core.config import APP_VERSION # For potential logging or response metadata

router = APIRouter()

@router.post("/scrape-capterra", tags=["Capterra"])
async def scrape_capterra_endpoint(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    start_date_filter_dt, end_date_filter_dt = None, None 
    if request.start_date_str:
        try: start_date_filter_dt = datetime.strptime(request.start_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str format (YYYY-MM-DD).")
    if request.end_date_str:
        try: end_date_filter_dt = datetime.strptime(request.end_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str format (YYYY-MM-DD).")
    if start_date_filter_dt and end_date_filter_dt and start_date_filter_dt > end_date_filter_dt: raise HTTPException(status_code=400, detail="Start date cannot be after end date.")
    if not request.urls: raise HTTPException(status_code=400, detail="No URLs provided.")
    results: Dict[str, Dict[str, Any]] = {}; valid_urls_for_processing = []; url_to_task_map = {} 
    for url_obj in request.urls:
        s_url = str(url_obj)
        if "capterra.com/p/" in s_url and "/reviews" in s_url: valid_urls_for_processing.append(url_obj)
        else: results[s_url] = {"status": "error", "message": f"Invalid Capterra URL format: {s_url} or skipped."}
    if not valid_urls_for_processing:
        if results: return results 
        raise HTTPException(status_code=400, detail="No valid Capterra URLs provided.")
    tasks = [asyncio.create_task(scrape_capterra_async(str(url), None, None), name=f"scrape_{str(url)}") for url in valid_urls_for_processing]
    for task, url_obj in zip(tasks, valid_urls_for_processing): url_to_task_map[task] = str(url_obj)
    task_execution_results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res_or_exc in enumerate(task_execution_results):
        original_url = url_to_task_map[tasks[i]] 
        if isinstance(res_or_exc, Exception):
            current_task_name = tasks[i].get_name()
            print(f"Task '{current_task_name}' (v{app.version}) EXCEPTION: {type(res_or_exc).__name__} - {res_or_exc}"); traceback.print_exc(file=sys.stdout)
            results[original_url] = {"status": "error", "message": f"Scraping task failed: {type(res_or_exc).__name__}."}
        else: results[original_url] = res_or_exc 
    return results