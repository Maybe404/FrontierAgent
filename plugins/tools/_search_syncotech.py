"""Syncotech web-search backend, normalised to the Serper response shape.

Enabled when ``SYNCO_SEARCH_URL`` and ``SYNCO_SEARCH_TOKEN`` are set; the
caller (``web_search._do_web_search``) then routes here instead of Serper, so
formatting, filtering and coalescing stay unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import os

import httpx

from frontier_agent.infra.usage_meter import record_api_request

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
# The backend returns full page text; keep the result list compact and let the
# agent call web_fetch for pages it actually wants to read.
_SNIPPET_CHARS = 1200


def enabled() -> bool:
    return bool(os.getenv("SYNCO_SEARCH_URL") and os.getenv("SYNCO_SEARCH_TOKEN"))


def _to_serper(data: dict) -> dict:
    organic = []
    for page in (data.get("data") or {}).get("webPageList") or []:
        content = (page.get("content") or page.get("markDownContent") or "").strip()
        organic.append({
            "title": page.get("title") or "",
            "link": page.get("url") or "",
            "snippet": content[:_SNIPPET_CHARS],
            "date": page.get("publishedDate") or "",
            "source": page.get("hostname") or page.get("siteName") or "",
        })
    return {"organic": organic}


async def search(query: str, num_results: int) -> dict:
    url = os.environ["SYNCO_SEARCH_URL"]
    headers = {
        "Authorization": os.environ["SYNCO_SEARCH_TOKEN"],
        "Content-Type": "application/json",
    }
    # Field names (appld, queyContext) are the backend's own spelling.
    payload = [{
        "appld": os.getenv("SYNCO_SEARCH_APP_ID", "1000002"),
        "num": str(num_results),
        "queyContext": query,
        "excludeList": [],
        "includeList": [],
    }]
    for attempt in range(_MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code == 429 or resp.status_code >= 500:
                logger.warning("Syncotech search HTTP %d, retrying (attempt %d)",
                               resp.status_code, attempt + 1)
                record_api_request("syncotech_search", requests=0, retries=1)
                await asyncio.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            body = resp.json()
            if body.get("code") not in (0, "0", None):
                logger.error("Syncotech search error code=%s msg=%s trace=%s",
                             body.get("code"), body.get("msg"), body.get("traceId"))
                record_api_request("syncotech_search", requests=0, errors=1)
                return {}
            record_api_request("syncotech_search")
            return _to_serper(body)
        except httpx.TimeoutException:
            logger.warning("Syncotech search timeout for '%s' (attempt %d)",
                           query[:50], attempt + 1)
            record_api_request("syncotech_search", requests=0, errors=1)
            await asyncio.sleep(1)
        except Exception as e:
            logger.error("Syncotech search failed for '%s': %s", query[:50], e)
            record_api_request("syncotech_search", requests=0, errors=1)
            return {}
    logger.error("Syncotech search exhausted retries for '%s'", query[:50])
    return {}
