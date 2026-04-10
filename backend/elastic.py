"""
elastic.py — Queries Elasticsearch for internal ad server data (Clever).
All index names and field names are configurable via environment variables.
"""

import os
import re
import json
import ipaddress
import atexit
import logging
import time as time_module
import httpx
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timezone
from dotenv import load_dotenv
from typing import Optional
from zoneinfo import ZoneInfo

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=True)

ES_URL      = os.environ.get("ES_URL", "http://localhost:9200").strip()
# Ensure URL has protocol
if ES_URL and not ES_URL.startswith(("http://", "https://")):
    ES_URL = f"https://{ES_URL}"
ES_URL = ES_URL.rstrip("/")
KIBANA_VERSION = os.environ.get("KIBANA_VERSION", "8.2.3")

ES_USERNAME = os.environ.get("ES_USERNAME", "elastic")
ES_PASSWORD = os.environ.get("ES_PASSWORD", "")
ES_INDICES  = os.environ.get("ES_INDICES", "served-impressions,core-view-events").split(",")

# Field name mappings — override via .env as needed
ES_DATE_FIELD      = os.environ.get("ES_DATE_FIELD", "@timestamp")
ES_PUBLISHER_FIELD = os.environ.get("ES_PUBLISHER_FIELD", "publisher")
ES_SERVED_FIELD    = os.environ.get("ES_SERVED_FIELD", "served_impressions")
ES_VIEWABLE_FIELD  = os.environ.get("ES_VIEWABLE_FIELD", "viewable_impressions")
ES_TIMEZONE = os.environ.get("ES_TIMEZONE", "Europe/Lisbon").strip() or "Europe/Lisbon"
ES_PUBLISHER_MATCH_MODE = os.environ.get("ES_PUBLISHER_MATCH_MODE", "contains").strip().lower()
ES_IP_FALLBACK_MAX_HITS = int(os.environ.get("ES_IP_FALLBACK_MAX_HITS", "8000"))
ES_BEHAVIOR_MAX_HITS = int(os.environ.get("ES_BEHAVIOR_MAX_HITS", "3000"))
ES_BEHAVIOR_BASE_HITS = int(os.environ.get("ES_BEHAVIOR_BASE_HITS", "500"))
ES_BEHAVIOR_MIN_HITS_FOR_NO_ESCALATION = int(os.environ.get("ES_BEHAVIOR_MIN_HITS_FOR_NO_ESCALATION", "400"))
ES_IP_TERMS_FALLBACK_SIZE = int(os.environ.get("ES_IP_TERMS_FALLBACK_SIZE", "30"))
ES_BEHAVIOR_MIN_COVERAGE_FOR_ESCALATION = float(os.environ.get("ES_BEHAVIOR_MIN_COVERAGE_FOR_ESCALATION", "0.15"))
ES_SKIP_SERVED_IP_AGG_FALLBACK = os.environ.get("ES_SKIP_SERVED_IP_AGG_FALLBACK", "true").strip().lower() in ("1", "true", "yes", "on")
ES_INCLUDE_UA_ORIGINAL_AGG = os.environ.get("ES_INCLUDE_UA_ORIGINAL_AGG", "false").strip().lower() in ("1", "true", "yes", "on")
SUSPICIOUS_CACHE_TTL_SECONDS = int(os.environ.get("SUSPICIOUS_CACHE_TTL_SECONDS", "120"))
SUSPICIOUS_CACHE_MAX_ITEMS = int(os.environ.get("SUSPICIOUS_CACHE_MAX_ITEMS", "64"))

logger = logging.getLogger(__name__)

try:
    ES_QUERY_TZ = ZoneInfo(ES_TIMEZONE)
except Exception:
    ES_QUERY_TZ = timezone.utc

_suspicious_cache: dict[tuple, tuple[float, dict]] = {}
_http_client: httpx.Client | None = None
_thread_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=8)


def _to_es_utc_timestamp(dt: datetime) -> str:
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"


def _parse_range_input(value: str, is_end: bool) -> datetime:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("Date range value cannot be empty")

    if "T" in raw:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ES_QUERY_TZ)
        return dt

    d = datetime.fromisoformat(raw)
    # End of range is exclusive: date-only end means start of that day.
    day_time = time(0, 0, 0, 0) if is_end else time(0, 0, 0, 0)
    return datetime.combine(d.date(), day_time, ES_QUERY_TZ)


def _to_utc_day_bounds(start_date: str, end_date: str) -> tuple[str, str]:
    """Return UTC bounds with end-exclusive semantics [start, end)."""
    start_dt = _parse_range_input(start_date, is_end=False)
    end_dt = _parse_range_input(end_date, is_end=True)

    return _to_es_utc_timestamp(start_dt), _to_es_utc_timestamp(end_dt)


def _build_script_id_filter(script_id: str) -> dict:
    sid = script_id.strip()
    should_clauses: list[dict] = [
        {"term": {"script_id": sid}},
        {"match": {"script_id": sid}},
    ]
    if sid.isdigit():
        should_clauses.insert(1, {"term": {"script_id": int(sid)}})

    return {
        "bool": {
            "should": should_clauses,
            "minimum_should_match": 1,
        }
    }


def _build_publisher_filter(publisher: str) -> dict:
    pub = publisher.strip()
    if ES_PUBLISHER_MATCH_MODE == "exact":
        return {"term": {f"{ES_PUBLISHER_FIELD}.keyword": pub}}

    return {
        "bool": {
            "should": [
                {"term": {f"{ES_PUBLISHER_FIELD}.keyword": pub}},
                {"wildcard": {f"{ES_PUBLISHER_FIELD}.keyword": f"*{pub}*"}},
                {"match": {ES_PUBLISHER_FIELD: pub}},
            ],
            "minimum_should_match": 1,
        }
    }

# HTTP client setup
def _get_auth():
    """Returns auth tuple if credentials are provided."""
    return (ES_USERNAME, ES_PASSWORD) if ES_PASSWORD else None


def _close_http_client() -> None:
    global _http_client
    if _http_client is None:
        return
    if not getattr(_http_client, "is_closed", False):
        _http_client.close()
    _http_client = None


def _close_thread_executor() -> None:
    try:
        _thread_executor.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass


def _get_client():
    """Returns a persistent HTTP client with pooling for reuse across requests."""
    global _http_client
    if _http_client is None or getattr(_http_client, "is_closed", False):
        _http_client = httpx.Client(
            verify=False,
            timeout=30.0,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
    return _http_client


atexit.register(_close_http_client)
atexit.register(_close_thread_executor)


def _is_kibana_url() -> bool:
    return "kibana" in ES_URL.lower()


def _should_skip_ip_agg_fallback(index: str, field: str) -> bool:
    if not ES_SKIP_SERVED_IP_AGG_FALLBACK:
        return False
    return field == "source.ip" and "served-impressions" in (index or "")


def _cache_get_suspicious(cache_key: tuple) -> dict | None:
    if SUSPICIOUS_CACHE_TTL_SECONDS <= 0:
        return None

    now = time_module.time()
    # Opportunistic cleanup of expired entries.
    expired_keys = [k for k, (exp, _) in _suspicious_cache.items() if exp <= now]
    for k in expired_keys:
        _suspicious_cache.pop(k, None)

    item = _suspicious_cache.get(cache_key)
    if not item:
        return None
    expires_at, value = item
    if expires_at <= now:
        _suspicious_cache.pop(cache_key, None)
        return None

    # Defensive copy to avoid accidental mutation of cached payload.
    return json.loads(json.dumps(value))


def _cache_set_suspicious(cache_key: tuple, value: dict) -> None:
    if SUSPICIOUS_CACHE_TTL_SECONDS <= 0:
        return

    if len(_suspicious_cache) >= max(SUSPICIOUS_CACHE_MAX_ITEMS, 1):
        oldest_key = min(_suspicious_cache.items(), key=lambda kv: kv[1][0])[0]
        _suspicious_cache.pop(oldest_key, None)

    expires_at = time_module.time() + SUSPICIOUS_CACHE_TTL_SECONDS
    _suspicious_cache[cache_key] = (expires_at, json.loads(json.dumps(value)))


def _kibana_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
        "kbn-version": KIBANA_VERSION,
    }


def _kibana_search(index: str, body: dict) -> dict:
    auth = _get_auth()
    payload = {"params": {"index": index, "body": body}}

    client = _get_client()
    response = client.post(
        f"{ES_URL}/internal/search/es",
        json=payload,
        auth=auth,
        headers=_kibana_headers(),
    )
    response.raise_for_status()

    data = response.json()
    return data.get("rawResponse", {})


def _kibana_count_query(
    start_date: str,
    end_date: str,
    publisher: str | None,
    script_id: str | None,
    event_type: str | None = None,
) -> dict:
    gte, lt = _to_utc_day_bounds(start_date, end_date)

    filters: list[dict] = [
        {
            "range": {
                ES_DATE_FIELD: {
                    "gte": gte,
                    "lt": lt,
                    "format": "strict_date_optional_time",
                }
            }
        }
    ]

    if script_id:
        sid = script_id.strip()
        if sid:
            filters.append(_build_script_id_filter(sid))
    elif publisher:
        pub = publisher.strip()
        if pub.isdigit():
            filters.append(_build_script_id_filter(pub))
        else:
            filters.append(_build_publisher_filter(pub))

    if event_type:
        filters.append({"match_phrase": {"type_name": event_type}})

    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
    }


def _extract_total_hits(raw: dict) -> int:
    hits_total = raw.get("hits", {}).get("total", 0)
    if isinstance(hits_total, dict):
        return int(hits_total.get("value", 0) or 0)
    return int(hits_total or 0)


def _query_via_kibana(start_date: str, end_date: str, publisher: str | None, script_id: str | None) -> dict:
    indices = get_available_indices()
    served_index = next((i for i in indices if "served" in i), indices[0])
    viewable_index = next((i for i in indices if "core-view-events" in i), served_index)

    served_raw = _kibana_search(
        served_index,
        _kibana_count_query(start_date, end_date, publisher, script_id, event_type=None),
    )
    viewable_raw = _kibana_search(
        viewable_index,
        _kibana_count_query(start_date, end_date, publisher, script_id, event_type="viewable_impression"),
    )
    garbage_raw = _kibana_search(
        viewable_index,
        _kibana_count_query(start_date, end_date, publisher, script_id, event_type="garbage"),
    )

    served_count = _extract_total_hits(served_raw)
    viewable_count = _extract_total_hits(viewable_raw)
    garbage_count = _extract_total_hits(garbage_raw)

    return {
        "served_impressions": served_count,
        "viewable_impressions": viewable_count,
        "garbage_impressions": garbage_count,
        "doc_count": served_count,
    }


def _build_type_name_count_query(
    start_date: str,
    end_date: str,
    publisher: str | None,
    script_id: str | None,
    event_type: str,
) -> dict:
    gte, lt = _to_utc_day_bounds(start_date, end_date)
    must_clauses: list[dict] = [
        {
            "range": {
                ES_DATE_FIELD: {
                    "gte": gte,
                    "lt": lt,
                    "format": "strict_date_optional_time",
                }
            }
        },
        {"match_phrase": {"type_name": event_type}},
    ]

    if script_id:
        sid = script_id.strip()
        if sid:
            must_clauses.append(_build_script_id_filter(sid))
    elif publisher:
        must_clauses.append(_build_publisher_filter(publisher))

    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"must": must_clauses}},
    }


def _count_type_name_events(
    index: str,
    start_date: str,
    end_date: str,
    publisher: str | None,
    script_id: str | None,
    event_type: str,
) -> int:
    query = _build_type_name_count_query(start_date, end_date, publisher, script_id, event_type)
    auth = _get_auth()

    client = _get_client()
    response = client.post(
        f"{ES_URL}/{index}/_search",
        json=query,
        auth=auth,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    data = response.json()
    hits_total = data.get("hits", {}).get("total", 0)
    if isinstance(hits_total, dict):
        return int(hits_total.get("value", 0) or 0)
    return int(hits_total or 0)


def _raise_actionable_es_error(exc: httpx.HTTPStatusError) -> None:
    """Convert low-level HTTP errors into actionable setup guidance."""
    status = exc.response.status_code
    request_url = str(exc.request.url)
    is_kibana_url = "kibana" in ES_URL.lower()

    if status == 404 and is_kibana_url:
        raise RuntimeError(
            "Configured ES_URL appears to be a Kibana URL, not the Elasticsearch HTTP endpoint. "
            f"Current ES_URL: {ES_URL}. "
            "Use the direct Elasticsearch host (for example https://<es-host>:9200)."
        ) from exc

    raise RuntimeError(f"Elasticsearch HTTP {status} for {request_url}: {exc.response.text[:200]}") from exc




def get_available_indices() -> list[str]:
    """Returns list of available indices from configuration."""
    return [idx.strip() for idx in ES_INDICES]


def get_index_fields(index: str) -> dict:
    """
    Fetches field mapping for an index and returns field information.
    
    Returns:
        {
            "fields": [
                {"name": "field_name", "type": "keyword"},
                {"name": "@timestamp", "type": "date"},
                ...
            ]
        }
    """
    auth = _get_auth()
    
    client = _get_client()
    try:
        response = client.get(
            f"{ES_URL}/{index}/_mapping",
            auth=auth,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_actionable_es_error(exc)
    
    data = response.json()
    
    # Extract field mappings
    mapping = data.get(index, {}).get("mappings", {}).get("properties", {})
    
    fields = []
    for field_name, field_info in mapping.items():
        field_type = field_info.get("type", "unknown")
        fields.append({
            "name": field_name,
            "type": field_type,
        })
    
    return {"fields": sorted(fields, key=lambda x: x["name"])}


def get_field_top_values(index: str, field: str, size: int = 5, filters: Optional[dict] = None) -> dict:
    """
    Fetches top N values for a specific field with their frequencies.
    
    Args:
        index: Index name
        field: Field name to aggregate on
        size: Number of top values to return
        filters: Optional dict with 'must_include' and 'must_exclude' lists of terms
    
    Returns:
        {
            "values": [
                {"value": "viewable_impression", "count": 1500, "percent": 71.0},
                {"value": "unload", "count": 320, "percent": 15.2},
                ...
            ],
            "total_docs": 2100
        }
    """
    # Determine the actual field to aggregate on (use .keyword for text fields)
    agg_field = f"{field}.keyword" if not field.startswith("@") else field
    
    query = {
        "size": 0,
        "query": {"bool": {"must": []}},
        "aggs": {
            "top_values": {
                "terms": {"field": agg_field, "size": size}
            }
        }
    }
    
    # Add filters if provided
    if filters:
        must_clauses = query["query"]["bool"]["must"]
        
        if filters.get("must_include"):
            must_clauses.append({
                "terms": {agg_field: filters["must_include"]}
            })
        
        if filters.get("must_exclude"):
            query["query"]["bool"]["must_not"] = {
                "terms": {agg_field: filters["must_exclude"]}
            }
    
    auth = _get_auth()
    
    client = _get_client()
    try:
        response = client.post(
            f"{ES_URL}/{index}/_search",
            json=query,
            auth=auth,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_actionable_es_error(exc)
    
    data = response.json()
    
    # Parse results
    aggs = data.get("aggregations", {})
    buckets = aggs.get("top_values", {}).get("buckets", [])
    total_docs = data.get("hits", {}).get("total", {}).get("value", 0)
    
    values = []
    for bucket in buckets:
        values.append({
            "value": bucket["key"],
            "count": bucket["doc_count"],
            "percent": round((bucket["doc_count"] / total_docs * 100) if total_docs > 0 else 0, 1),
        })
    
    return {
        "values": values,
        "total_docs": total_docs,
    }


def build_advanced_query(
    index: str,
    start_date: str,
    end_date: str,
    filters: Optional[dict] = None,
) -> dict:
    """
    Builds an advanced Elasticsearch query with date range and multiple field filters.
    
    Args:
        index: Index name
        start_date: Start date (yyyy-MM-dd)
        end_date: End date (yyyy-MM-dd)
        filters: Dict with field filters
            Example:
            {
                "script_id": {"must_include": [10000]},
                "type_name": {"must_include": ["viewable_impression"], "must_exclude": []},
            }
    
    Returns:
        Elasticsearch query dict
    """
    gte, lt = _to_utc_day_bounds(start_date, end_date)

    must_clauses = [
        {
            "range": {
                ES_DATE_FIELD: {
                    "gte": gte,
                    "lt": lt,
                    "format": "strict_date_optional_time",
                }
            }
        }
    ]
    
    must_not_clauses = []

    # Populate include/exclude clauses in a single pass.
    if filters:
        for field_name, filter_spec in filters.items():
            agg_field = f"{field_name}.keyword" if not field_name.startswith("@") else field_name

            if filter_spec.get("must_include"):
                must_clauses.append({
                    "terms": {agg_field: filter_spec["must_include"]}
                })
            if filter_spec.get("must_exclude"):
                must_not_clauses.append({
                    "terms": {agg_field: filter_spec["must_exclude"]}
                })
    
    return {
        "size": 0,
        "query": {
            "bool": {
                "must": must_clauses,
                **({"must_not": must_not_clauses} if must_not_clauses else {}),
            }
        },
    }


def _build_query(start_date: str, end_date: str, publisher: str | None, script_id: str | None) -> dict:
    """Build an Elasticsearch aggregation query."""
    gte, lt = _to_utc_day_bounds(start_date, end_date)

    must_clauses = [
        {
            "range": {
                ES_DATE_FIELD: {
                    "gte": gte,
                    "lt": lt,
                    "format": "strict_date_optional_time",
                }
            }
        }
    ]

    if script_id:
        sid = script_id.strip()
        if sid:
            must_clauses.append(_build_script_id_filter(sid))
    elif publisher:
        must_clauses.append(_build_publisher_filter(publisher))

    return {
        "size": 0,
        "query": {
            "bool": {
                "must": must_clauses
            }
        },
        "aggs": {
            "total_served": {
                "sum": {"field": ES_SERVED_FIELD}
            },
            "total_viewable": {
                "sum": {"field": ES_VIEWABLE_FIELD}
            },
        },
    }


def query_elasticsearch(
    index: str,
    start_date: str,
    end_date: str,
    publisher: str | None = None,
    script_id: str | None = None,
) -> dict:
    """
    Queries Elasticsearch and returns aggregated served and viewable impressions.
    
    Args:
        index: Index name to query
        start_date: Start date (yyyy-MM-dd)
        end_date: End date (yyyy-MM-dd)
        publisher: Optional publisher filter
        script_id: Optional script_id filter (preferred when provided)

    Returns:
        {
            "served_impressions": int,
            "viewable_impressions": int,
            "garbage_impressions": int,
            "doc_count": int,
        }
    Raises httpx.HTTPStatusError on bad responses.
    """
    if _is_kibana_url():
        return _query_via_kibana(start_date, end_date, publisher, script_id)

    query = _build_query(start_date, end_date, publisher, script_id)

    auth = _get_auth()

    client = _get_client()
    try:
        response = client.post(
            f"{ES_URL}/{index}/_search",
            json=query,
            auth=auth,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_actionable_es_error(exc)

    data = response.json()

    aggs = data.get("aggregations", {})
    served   = int(aggs.get("total_served",   {}).get("value", 0) or 0)
    viewable = int(aggs.get("total_viewable", {}).get("value", 0) or 0)
    doc_count = data.get("hits", {}).get("total", {}).get("value", 0)

    indices = get_available_indices()
    events_index = next((i for i in indices if "core-view-events" in i), index)
    try:
        garbage = _count_type_name_events(events_index, start_date, end_date, publisher, script_id, "garbage")
    except httpx.HTTPStatusError:
        garbage = 0

    return {
        "served_impressions": served,
        "viewable_impressions": viewable,
        "garbage_impressions": garbage,
        "doc_count": doc_count,
    }


def _run_search(index: str, body: dict, timeout_seconds: float | None = None) -> dict:
    """Run search either via Kibana internal API or direct Elasticsearch."""
    if _is_kibana_url():
        return _kibana_search(index, body)

    auth = _get_auth()
    client = _get_client()
    response = client.post(
        f"{ES_URL}/{index}/_search",
        json=body,
        auth=auth,
        headers={"Content-Type": "application/json"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def _run_search_safe(index: str, body: dict, timeout_seconds: float | None = None) -> dict | None:
    """Run search and return None on recoverable request errors (best-effort path)."""
    try:
        return _run_search(index, body, timeout_seconds=timeout_seconds)
    except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.RequestError):
        return None


def _run_msearch_safe(requests: list[tuple[str, dict]]) -> list[dict | None] | None:
    """
    Best-effort msearch.
    Returns one response per request, preserving order. Returns None when unavailable.
    """
    if not requests:
        return []

    # Kibana internal proxy in this project is search-only; fallback to per-index _search.
    if _is_kibana_url():
        return None

    auth = _get_auth()
    lines: list[str] = []
    for index, body in requests:
        lines.append(json.dumps({"index": index}))
        lines.append(json.dumps(body))
    payload = "\n".join(lines) + "\n"

    try:
        client = _get_client()
        response = client.post(
            f"{ES_URL}/_msearch",
            content=payload,
            auth=auth,
            headers={"Content-Type": "application/x-ndjson"},
        )
        response.raise_for_status()

        data = response.json()
        raw_items = data.get("responses", [])
        results: list[dict | None] = []
        for item in raw_items:
            if not isinstance(item, dict) or item.get("error"):
                results.append(None)
            else:
                results.append(item)

        while len(results) < len(requests):
            results.append(None)
        return results
    except Exception:
        return None


def _run_searches_parallel(requests: list[tuple[str, dict]]) -> list[dict | None]:
    """
    Run multiple searches in parallel when msearch is unavailable.
    Preserves request order and avoids serial latency.
    """
    if not requests:
        return []

    results: list[dict | None] = [None] * len(requests)
    futures = [_thread_executor.submit(_run_search_safe, index, body) for index, body in requests]
    for idx, future in enumerate(futures):
        try:
            results[idx] = future.result()
        except Exception:
            results[idx] = None
    return results


def _percent(part: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return round((float(part) / float(total)) * 100.0, 2)


def _build_base_filters(
    start_date: str,
    end_date: str,
    publisher: str | None,
    script_id: str | None,
    event_types: list[str] | None = None,
    gte: str | None = None,
    lt: str | None = None,
) -> list[dict]:
    resolved_gte = gte
    resolved_lt = lt
    if not resolved_gte or not resolved_lt:
        resolved_gte, resolved_lt = _to_utc_day_bounds(start_date, end_date)
    filters: list[dict] = [
        {
            "range": {
                ES_DATE_FIELD: {
                    "gte": resolved_gte,
                    "lt": resolved_lt,
                    "format": "strict_date_optional_time",
                }
            }
        }
    ]

    if script_id:
        sid = script_id.strip()
        if sid:
            filters.append(_build_script_id_filter(sid))
    elif publisher:
        pub = publisher.strip()
        if pub:
            filters.append(_build_publisher_filter(pub))

    if event_types:
        if len(event_types) == 1:
            filters.append({"match_phrase": {"type_name": event_types[0]}})
        else:
            filters.append(
                {
                    "bool": {
                        "should": [{"match_phrase": {"type_name": t}} for t in event_types],
                        "minimum_should_match": 1,
                    }
                }
            )

    return filters


def _field_to_agg_name(field: str) -> str:
    return field.replace(".", "_").replace("@", "at_")


def _agg_field_for_terms(field: str) -> str:
    """
    Return an aggregation-safe field name.
    Text-like fields are queried via .keyword to avoid fielddata errors.
    """
    if field.endswith(".keyword"):
        return field

    # Known non-text aggregatable fields.
    exact_fields = {
        "os.is_mobile",
        "device_type",
        "source.ip",
        "client.ip",
        "network.client.ip",
        "request.ip",
        ES_DATE_FIELD,
    }
    if field in exact_fields:
        return field

    # Heuristic: IP-like fields are normally mapped as `ip` and should not use `.keyword`.
    if field.endswith(".ip") or field == "ip" or field.endswith("_ip"):
        return field

    # Default for text-ish dimensions.
    return f"{field}.keyword"


def _missing_value_for_terms(field: str) -> str | None:
    """Return a safe `missing` value for terms aggregations by field type hint."""
    # Boolean fields cannot parse the string marker "__EMPTY__".
    boolean_fields = {
        "os.is_mobile",
    }
    if field in boolean_fields:
        return None
    return "__EMPTY__"


def _build_suspicious_query(filters: list[dict], fields: list[str]) -> dict:
    aggs: dict = {
        "per_minute": {
            "date_histogram": {
                "field": ES_DATE_FIELD,
                "fixed_interval": "1m",
                "min_doc_count": 0,
            }
        },
        "unique_ip": {
            "cardinality": {
                "field": "source.ip"
            }
        },
    }

    for f in fields:
        missing_value = _missing_value_for_terms(f)
        terms_obj = {
            "field": _agg_field_for_terms(f),
            "size": 10,
        }
        if missing_value is not None:
            terms_obj["missing"] = missing_value
        aggs[_field_to_agg_name(f)] = {
            "terms": terms_obj
        }

    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": aggs,
    }


def _build_terms_query(filters: list[dict], field: str, size: int = 10, agg_field: str | None = None) -> dict:
    agg_name = _field_to_agg_name(field)
    selected_field = agg_field or _agg_field_for_terms(field)
    missing_value = _missing_value_for_terms(field)
    terms_obj = {
        "field": selected_field,
        "size": size,
    }
    if missing_value is not None:
        terms_obj["missing"] = missing_value
    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            agg_name: {
                "terms": terms_obj
            }
        },
    }


def _build_cardinality_query(filters: list[dict], field: str, agg_field: str | None = None) -> dict:
    selected_field = agg_field or _agg_field_for_terms(field)
    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            "unique_value": {
                "cardinality": {
                    "field": selected_field,
                }
            }
        },
    }


def _build_histogram_query(filters: list[dict]) -> dict:
    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            "per_minute": {
                "date_histogram": {
                    "field": ES_DATE_FIELD,
                    "fixed_interval": "1m",
                    "min_doc_count": 0,
                }
            }
        },
    }


def _build_consolidated_aggs_query(
    filters: list[dict],
    term_specs: list[tuple[str, str, int]],
    cardinality_specs: list[tuple[str, str]],
    include_histogram: bool = True,
) -> dict:
    aggs: dict = {}

    for agg_name, field, size in term_specs:
        missing_value = _missing_value_for_terms(field)
        terms_obj = {
            "field": _agg_field_for_terms(field),
            "size": size,
        }
        if missing_value is not None:
            terms_obj["missing"] = missing_value
        aggs[agg_name] = {
            "terms": terms_obj
        }

    for agg_name, field in cardinality_specs:
        cardinality_obj: dict = {
            "field": _agg_field_for_terms(field),
        }
        if field == "source.ip":
            cardinality_obj["precision_threshold"] = 3000
        aggs[agg_name] = {
            "cardinality": cardinality_obj
        }

    if include_histogram:
        aggs["per_minute"] = {
            "date_histogram": {
                "field": ES_DATE_FIELD,
                "fixed_interval": "1m",
                "min_doc_count": 0,
            }
        }

    # Global per-IP daily volume signals (top IPs only) to avoid sampling bias.
    aggs["ip_daily_volume"] = {
        "terms": {
            "field": "source.ip",
            "size": 200,
            "order": {"_count": "desc"},
        },
        "aggs": {
            "per_day": {
                "date_histogram": {
                    "field": ES_DATE_FIELD,
                    "calendar_interval": "1d",
                    "min_doc_count": 1,
                    "time_zone": ES_TIMEZONE,
                }
            }
        },
    }

    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": aggs,
    }


def _extract_terms_from_agg(raw: dict, agg_name: str, total_docs: int) -> list[dict]:
    buckets = raw.get("aggregations", {}).get(agg_name, {}).get("buckets", []) if raw else []
    values: list[dict] = []
    for b in buckets:
        key = str(b.get("key", ""))
        count = int(b.get("doc_count", 0) or 0)
        if not key:
            key = "__EMPTY__"
        values.append(
            {
                "value": key,
                "count": count,
                "percent": round((count / total_docs * 100) if total_docs > 0 else 0, 2),
            }
        )
    return values


def _extract_cardinality_from_agg(raw: dict, agg_name: str) -> int:
    if not raw:
        return 0
    return int(raw.get("aggregations", {}).get(agg_name, {}).get("value", 0) or 0)


def _extract_histogram_from_agg(raw: dict, agg_name: str = "per_minute") -> list[int]:
    if not raw:
        return []
    buckets = raw.get("aggregations", {}).get(agg_name, {}).get("buckets", [])
    return [int(b.get("doc_count", 0) or 0) for b in buckets]


def _extract_daily_volume_from_agg(raw: dict, agg_name: str) -> dict:
    by_user_day = _extract_ip_daily_counts_from_agg(raw, agg_name)
    return _compute_daily_volume_from_user_day(by_user_day)


def _is_daily_volume_empty(volume: dict) -> bool:
    return all(int(volume.get(k, 0) or 0) == 0 for k in (
        "records_over_20",
        "records_over_50",
        "records_over_100",
        "repeat_users_over_20_days",
        "repeat_users_over_50_days",
        "max_events_single_user_day",
        "weighted_excess",
    ))


def _fetch_terms_distribution(index: str, filters: list[dict], field: str, total_docs: int, size: int = 500) -> list[dict]:
    if _should_skip_ip_agg_fallback(index, field):
        return []

    logger.warning("Terms fallback triggered for field=%s index=%s", field, index)

    candidate_seed = [_agg_field_for_terms(field)]
    if not field.endswith(".keyword"):
        candidate_seed.extend([f"{field}.keyword", field])

    candidates: list[str] = []
    for candidate in candidate_seed:
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    last_terms: list[dict] = []
    for agg_field in candidates:
        raw = _run_search_safe(index, _build_terms_query(filters, field, size=size, agg_field=agg_field))
        if not raw:
            continue
        terms = _extract_terms_from_agg(raw, _field_to_agg_name(field), total_docs)
        if any(t.get("value") != "__EMPTY__" for t in terms):
            return terms
        last_terms = terms

    return last_terms


def _fetch_unique_count(index: str, filters: list[dict], field: str) -> int:
    if _should_skip_ip_agg_fallback(index, field):
        return 0

    candidates: list[str] = []
    primary = _agg_field_for_terms(field)
    candidates.append(primary)
    if not field.endswith(".keyword"):
        alt = f"{field}.keyword"
        if primary != alt:
            candidates.append(alt)
        if primary != field:
            candidates.append(field)

    last_value = 0
    for agg_field in dict.fromkeys(candidates):
        raw = _run_search_safe(index, _build_cardinality_query(filters, field, agg_field=agg_field))
        if not raw:
            continue
        value = int(raw.get("aggregations", {}).get("unique_value", {}).get("value", 0) or 0)
        if value > 0:
            return value
        last_value = value

    return last_value


def _fetch_histogram_counts(index: str, filters: list[dict], timeout_seconds: float = 5.0) -> list[int]:
    raw = _run_search_safe(index, _build_histogram_query(filters), timeout_seconds=timeout_seconds)
    if not raw:
        logger.warning("Histogram fallback unavailable for index=%s (timeout/error)", index)
        return []
    buckets = raw.get("aggregations", {}).get("per_minute", {}).get("buckets", [])
    return [int(b.get("doc_count", 0) or 0) for b in buckets]


def _compute_daily_volume_from_user_day(by_user_day: dict[tuple[str, str], int]) -> dict:
    empty = {
        "records_over_20": 0,
        "records_over_50": 0,
        "records_over_100": 0,
        "repeat_users_over_20_days": 0,
        "repeat_users_over_50_days": 0,
        "max_events_single_user_day": 0,
        "weighted_excess": 0,
    }
    if not by_user_day:
        return empty

    counts = list(by_user_day.values())
    records_over_20 = sum(1 for c in counts if c > 20)
    records_over_50 = sum(1 for c in counts if c > 50)
    records_over_100 = sum(1 for c in counts if c > 100)

    user_days_over_20: dict[str, int] = {}
    user_days_over_50: dict[str, int] = {}
    for (ip, _), count in by_user_day.items():
        if count > 20:
            user_days_over_20[ip] = user_days_over_20.get(ip, 0) + 1
        if count > 50:
            user_days_over_50[ip] = user_days_over_50.get(ip, 0) + 1

    repeat_users_over_20_days = sum(1 for days in user_days_over_20.values() if days >= 2)
    repeat_users_over_50_days = sum(1 for days in user_days_over_50.values() if days >= 2)

    weighted_excess = 0
    for c in counts:
        weighted_excess += max(0, c - 20)
        weighted_excess += max(0, c - 50) * 2
        weighted_excess += max(0, c - 100) * 3

    return {
        "records_over_20": records_over_20,
        "records_over_50": records_over_50,
        "records_over_100": records_over_100,
        "repeat_users_over_20_days": repeat_users_over_20_days,
        "repeat_users_over_50_days": repeat_users_over_50_days,
        "max_events_single_user_day": max(counts),
        "weighted_excess": int(weighted_excess),
    }


def _extract_ip_daily_counts_from_agg(raw: dict, agg_name: str) -> dict[tuple[str, str], int]:
    by_user_day: dict[tuple[str, str], int] = {}
    if not raw:
        return by_user_day

    ip_buckets = raw.get("aggregations", {}).get(agg_name, {}).get("buckets", [])
    for ip_bucket in ip_buckets:
        ip = _normalize_ip_value(ip_bucket.get("key"))
        if not _is_valid_ip(ip):
            continue
        day_buckets = ip_bucket.get("per_day", {}).get("buckets", [])
        for day_bucket in day_buckets:
            day_key = str(day_bucket.get("key_as_string", "")).split("T", 1)[0]
            if not day_key:
                continue
            count = int(day_bucket.get("doc_count", 0) or 0)
            if count <= 0:
                continue
            by_user_day[(ip, day_key)] = count

    return by_user_day


def _build_user_day_counts_from_hits(hits: list[dict]) -> dict[tuple[str, str], int]:
    by_user_day: dict[tuple[str, str], int] = {}
    for hit in hits:
        src = hit.get("_source", {})
        ip = _extract_source_ip(src)
        ts = _parse_event_timestamp(src.get(ES_DATE_FIELD))
        if not ip or ts is None:
            continue
        day_key = ts.date().isoformat()
        key = (str(ip), day_key)
        by_user_day[key] = by_user_day.get(key, 0) + 1
    return by_user_day


def _parse_event_timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def _fetch_behavior_hits(index: str, filters: list[dict], max_hits: int = 8000) -> list[dict]:
    query = {
        "size": max_hits,
        "track_total_hits": False,
        "_source": [
            "source.ip", "source_ip", "ip",
            "client.ip", "client_ip", "network.client.ip", "request.ip",
            ES_DATE_FIELD,
        ],
        "query": {"bool": {"filter": filters}},
    }
    raw = _run_search_safe(index, query)
    if not raw:
        return []
    return raw.get("hits", {}).get("hits", [])


def _compute_avg_seconds_between_events_per_user(
    index: str,
    filters: list[dict],
    max_hits: int = 8000,
    hits: list[dict] | None = None,
) -> float:
    if hits is None:
        hits = _fetch_behavior_hits(index, filters, max_hits=max_hits)
    if not hits:
        return 0.0

    events: list[tuple[datetime, str]] = []
    for hit in hits:
        src = hit.get("_source", {})
        ip = _extract_source_ip(src)
        if not ip:
            continue
        ts = _parse_event_timestamp(src.get(ES_DATE_FIELD))
        if ts is None:
            continue
        events.append((ts, ip))

    if len(events) < 2:
        return 0.0

    # Keep temporal math deterministic without requiring ES-side sort (faster on large shards).
    events.sort(key=lambda x: x[0])

    prev_by_ip: dict[str, datetime] = {}
    deltas: list[float] = []

    for ts, ip in events:
        if ip in prev_by_ip:
            delta = (ts - prev_by_ip[ip]).total_seconds()
            if delta >= 0:
                deltas.append(delta)
        prev_by_ip[ip] = ts

    if not deltas:
        return 0.0
    return round(sum(deltas) / len(deltas), 3)


def _compute_avg_seconds_between_events(hits: list[dict]) -> float:
    if not hits:
        return 0.0

    timestamps: list[datetime] = []
    for hit in hits:
        src = hit.get("_source", {})
        ts = _parse_event_timestamp(src.get(ES_DATE_FIELD))
        if ts is not None:
            timestamps.append(ts)

    if len(timestamps) < 2:
        return 0.0

    timestamps.sort()
    deltas: list[float] = []
    prev_ts = timestamps[0]
    for ts in timestamps[1:]:
        delta = (ts - prev_ts).total_seconds()
        if delta >= 0:
            deltas.append(delta)
        prev_ts = ts

    if not deltas:
        return 0.0
    return round(sum(deltas) / len(deltas), 3)


def _compute_daily_user_volume_signals(
    index: str,
    filters: list[dict],
    max_hits: int = 8000,
    hits: list[dict] | None = None,
) -> dict:
    if hits is None:
        hits = _fetch_behavior_hits(index, filters, max_hits=max_hits)
    by_user_day = _build_user_day_counts_from_hits(hits)
    return _compute_daily_volume_from_user_day(by_user_day)


def _compute_behavior_signals_from_hits(hits: list[dict], total_docs: int) -> dict:
    """
    Single-pass behavior summarization to avoid multiple scans over the same hit set.
    Returns IP summaries, cadence metrics, and daily user-volume signals.
    """
    empty_daily = {
        "records_over_20": 0,
        "records_over_50": 0,
        "records_over_100": 0,
        "repeat_users_over_20_days": 0,
        "repeat_users_over_50_days": 0,
        "max_events_single_user_day": 0,
        "weighted_excess": 0,
    }
    if not hits:
        return {
            "top10_ips": [],
            "top10_ua_name": [],
            "top10_ua_original": [],
            "suspicious_ua_event_count": 0,
            "unique_ip_count": 0,
            "valid_ip_events": 0,
            "avg_seconds_between_events": 0.0,
            "avg_seconds_between_events_per_user": 0.0,
            "daily_user_volume": empty_daily,
        }

    ip_counts: dict[str, int] = {}
    ua_name_counts: dict[str, int] = {}
    ua_original_counts: dict[str, int] = {}
    suspicious_ua_event_count = 0
    valid_ip_events = 0
    timestamps: list[datetime] = []
    ip_ts_events: list[tuple[datetime, str]] = []
    by_user_day: dict[tuple[str, str], int] = {}
    suspicious_ua_pattern = re.compile(r"bot|crawler|spider|headless|phantom|selenium|python|curl|wget|scrapy", re.IGNORECASE)

    for hit in hits:
        src = hit.get("_source", {})
        ts = _parse_event_timestamp(src.get(ES_DATE_FIELD))
        if ts is not None:
            timestamps.append(ts)

        ua_name = _extract_user_agent_value(src, "name")
        if ua_name:
            ua_name_counts[ua_name] = ua_name_counts.get(ua_name, 0) + 1

        ua_original = _extract_user_agent_value(src, "original")
        if ua_original:
            ua_original_counts[ua_original] = ua_original_counts.get(ua_original, 0) + 1

        ua_text = ua_original or ua_name or ""
        if ua_text and suspicious_ua_pattern.search(ua_text):
            suspicious_ua_event_count += 1

        ip = _extract_source_ip(src)
        if not ip:
            continue

        valid_ip_events += 1
        ip_counts[ip] = ip_counts.get(ip, 0) + 1

        if ts is not None:
            ip_ts_events.append((ts, ip))
            day_key = ts.date().isoformat()
            key = (ip, day_key)
            by_user_day[key] = by_user_day.get(key, 0) + 1

    if ip_counts:
        ordered = sorted(ip_counts.items(), key=lambda kv: kv[1], reverse=True)
        top10_ips = [
            {
                "value": ip,
                "count": cnt,
                "percent": round((cnt / total_docs * 100) if total_docs > 0 else 0.0, 2),
            }
            for ip, cnt in ordered[:10]
        ]
        unique_ip_count = len(ip_counts)
    else:
        top10_ips = []
        unique_ip_count = 0

    if ua_name_counts:
        ordered_ua_name = sorted(ua_name_counts.items(), key=lambda kv: kv[1], reverse=True)
        top10_ua_name = [
            {
                "value": ua,
                "count": cnt,
                "percent": round((cnt / total_docs * 100) if total_docs > 0 else 0.0, 2),
            }
            for ua, cnt in ordered_ua_name[:10]
        ]
    else:
        top10_ua_name = []

    if ua_original_counts:
        ordered_ua_original = sorted(ua_original_counts.items(), key=lambda kv: kv[1], reverse=True)
        top10_ua_original = [
            {
                "value": ua,
                "count": cnt,
                "percent": round((cnt / total_docs * 100) if total_docs > 0 else 0.0, 2),
            }
            for ua, cnt in ordered_ua_original[:10]
        ]
    else:
        top10_ua_original = []

    if len(timestamps) >= 2:
        timestamps.sort()
        deltas = []
        prev_ts = timestamps[0]
        for ts in timestamps[1:]:
            delta = (ts - prev_ts).total_seconds()
            if delta >= 0:
                deltas.append(delta)
            prev_ts = ts
        avg_seconds_between_events = round(sum(deltas) / len(deltas), 3) if deltas else 0.0
    else:
        avg_seconds_between_events = 0.0

    if len(ip_ts_events) >= 2:
        ip_ts_events.sort(key=lambda x: x[0])
        prev_by_ip: dict[str, datetime] = {}
        user_deltas: list[float] = []
        for ts, ip in ip_ts_events:
            if ip in prev_by_ip:
                delta = (ts - prev_by_ip[ip]).total_seconds()
                if delta >= 0:
                    user_deltas.append(delta)
            prev_by_ip[ip] = ts
        avg_seconds_between_events_per_user = round(sum(user_deltas) / len(user_deltas), 3) if user_deltas else 0.0
    else:
        avg_seconds_between_events_per_user = 0.0

    if by_user_day:
        counts = list(by_user_day.values())
        records_over_20 = sum(1 for c in counts if c > 20)
        records_over_50 = sum(1 for c in counts if c > 50)
        records_over_100 = sum(1 for c in counts if c > 100)

        user_days_over_20: dict[str, int] = {}
        user_days_over_50: dict[str, int] = {}
        for (ip, _), count in by_user_day.items():
            if count > 20:
                user_days_over_20[ip] = user_days_over_20.get(ip, 0) + 1
            if count > 50:
                user_days_over_50[ip] = user_days_over_50.get(ip, 0) + 1

        repeat_users_over_20_days = sum(1 for days in user_days_over_20.values() if days >= 2)
        repeat_users_over_50_days = sum(1 for days in user_days_over_50.values() if days >= 2)

        weighted_excess = 0
        for c in counts:
            weighted_excess += max(0, c - 20)
            weighted_excess += max(0, c - 50) * 2
            weighted_excess += max(0, c - 100) * 3

        daily_user_volume = {
            "records_over_20": records_over_20,
            "records_over_50": records_over_50,
            "records_over_100": records_over_100,
            "repeat_users_over_20_days": repeat_users_over_20_days,
            "repeat_users_over_50_days": repeat_users_over_50_days,
            "max_events_single_user_day": max(counts),
            "weighted_excess": int(weighted_excess),
        }
    else:
        daily_user_volume = empty_daily

    return {
        "top10_ips": top10_ips,
        "top10_ua_name": top10_ua_name,
        "top10_ua_original": top10_ua_original,
        "suspicious_ua_event_count": suspicious_ua_event_count,
        "unique_ip_count": unique_ip_count,
        "valid_ip_events": valid_ip_events,
        "avg_seconds_between_events": avg_seconds_between_events,
        "avg_seconds_between_events_per_user": avg_seconds_between_events_per_user,
        "daily_user_volume": daily_user_volume,
    }


def _compute_uniformity_score(per_minute_counts: list[int]) -> float:
    non_zero = [v for v in per_minute_counts if v > 0]
    if len(non_zero) < 3:
        return 0.0
    mean = sum(non_zero) / len(non_zero)
    if mean <= 0:
        return 0.0
    variance = sum((v - mean) ** 2 for v in non_zero) / len(non_zero)
    std_dev = variance ** 0.5
    # Lower coefficient of variation = more uniform/robotic cadence.
    return round(std_dev / mean, 4)


def _merge_top_counts(a: list[dict], b: list[dict]) -> list[dict]:
    merged: dict[str, int] = {}
    for item in a + b:
        key = str(item.get("value", "__EMPTY__"))
        merged[key] = merged.get(key, 0) + int(item.get("count", 0) or 0)

    ordered = sorted(merged.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return [{"value": k, "count": v} for k, v in ordered]


def _normalize_ip_value(raw: object) -> str | None:
    if raw is None:
        return None

    if isinstance(raw, (list, tuple, set)):
        if not raw:
            return None
        raw = next(iter(raw))

    text = str(raw).strip().strip("'\"")
    if not text:
        return None

    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].strip().strip("'\"")

    # Try direct parse first.
    if _is_valid_ip(text):
        return text

    # Recover IP tokens from noisy values, e.g. "2a06:98c0:3600::103 ('ips')".
    for token in re.split(r"[\s,;()\[\]{}]+", text):
        token = token.strip().strip("'\"")
        if _is_valid_ip(token):
            return token

    return None


def _is_valid_ip(value: str | None) -> bool:
    if not value:
        return False
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _extract_source_ip(src: dict) -> str | None:
    candidates = [
        src.get("source", {}).get("ip") if isinstance(src.get("source"), dict) else None,
        src.get("source.ip"),
        src.get("source_ip"),
        src.get("ip"),
        src.get("client", {}).get("ip") if isinstance(src.get("client"), dict) else None,
        src.get("client.ip"),
        src.get("client_ip"),
        src.get("network", {}).get("client", {}).get("ip") if isinstance(src.get("network"), dict) else None,
        src.get("network.client.ip"),
        src.get("request", {}).get("ip") if isinstance(src.get("request"), dict) else None,
        src.get("request.ip"),
    ]
    for candidate in candidates:
        normalized = _normalize_ip_value(candidate)
        if _is_valid_ip(normalized):
            return normalized
    return None


def _extract_user_agent_value(src: dict, field: str) -> str | None:
    ua_obj = src.get("user_agent")
    if isinstance(ua_obj, dict):
        value = ua_obj.get(field)
        if value is not None:
            text = str(value).strip()
            return text if text else None

    dotted = src.get(f"user_agent.{field}")
    if dotted is not None:
        text = str(dotted).strip()
        return text if text else None

    return None


def _filter_valid_ip_buckets(items: list[dict]) -> list[dict]:
    valid: list[dict] = []
    for item in items:
        value = _normalize_ip_value(item.get("value"))
        if not _is_valid_ip(value):
            continue
        valid.append({
            "value": value,
            "count": int(item.get("count", 0) or 0),
            "percent": float(item.get("percent", 0.0) or 0.0),
        })
    return valid


def _top_n_pct(items: list[dict], total: int, n: int) -> float:
    if total <= 0 or not items:
        return 0.0
    top_sum = sum(int(i.get("count", 0) or 0) for i in items[:n])
    return _percent(top_sum, total)


def _repeated_ip_traffic_pct(top_ips: list[dict], total_events: int) -> float | None:
    if total_events <= 0 or not top_ips:
        return None
    repeated_events = sum(
        int(item.get("count", 0) or 0)
        for item in top_ips
        if int(item.get("count", 0) or 0) > 1
    )
    return _percent(repeated_events, total_events)


def _summarize_ip_from_local_hits(
    hits: list[dict],
    total_docs: int,
) -> tuple[list[dict], int, int]:
    """
    Compute top IPs and unique count directly from pre-fetched hits.
    Eliminates redundant ES query when behavior hits are already fetched.
    """
    if not hits:
        return [], 0, 0

    counts: dict[str, int] = {}
    valid_events = 0
    for hit in hits:
        src = hit.get("_source", {})
        ip = _extract_source_ip(src)
        if not ip:
            continue
        valid_events += 1
        counts[ip] = counts.get(ip, 0) + 1

    if not counts:
        return [], 0, 0

    ordered = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    unique_count = len(counts)
    
    top10 = [
        {
            "value": ip,
            "count": cnt,
            "percent": round((cnt / total_docs * 100) if total_docs > 0 else 0.0, 2),
        }
        for ip, cnt in ordered[:10]
    ]
    return top10, unique_count, valid_events


def analyze_suspicious_traffic(
    start_date: str,
    end_date: str,
    publisher: str | None,
    script_id: str | None,
    served_impressions: int,
    viewable_impressions: int,
    garbage_impressions: int,
) -> dict:
    """
    Deterministic suspicious-traffic analysis using served-impressions for Served and
    core-view-events for Viewable/Garbage behavior.
    
    Optimized: Fetches behavior hits upfront in parallel, reusing them for IP metrics
    and cadence calculations to eliminate redundant ES queries.
    """
    analysis_started_at = time_module.perf_counter()
    start_dt = _parse_range_input(start_date, is_end=False)
    end_dt = _parse_range_input(end_date, is_end=True)
    gte = _to_es_utc_timestamp(start_dt)
    lt = _to_es_utc_timestamp(end_dt)
    window_seconds = max(int((end_dt - start_dt).total_seconds()), 1)

    cache_key = (
        start_date,
        end_date,
        publisher or "",
        script_id or "",
        int(ES_BEHAVIOR_BASE_HITS),
        int(ES_BEHAVIOR_MAX_HITS),
        int(ES_BEHAVIOR_MIN_HITS_FOR_NO_ESCALATION),
        int(ES_IP_TERMS_FALLBACK_SIZE),
        bool(ES_INCLUDE_UA_ORIGINAL_AGG),
    )
    cached = _cache_get_suspicious(cache_key)
    if cached is not None:
        cache_elapsed_ms = round((time_module.perf_counter() - analysis_started_at) * 1000, 1)
        metrics = cached.get("metrics") if isinstance(cached, dict) else None
        if isinstance(metrics, dict):
            metrics["cache_hit"] = True
            cached_timings = metrics.get("timings_ms") if isinstance(metrics.get("timings_ms"), dict) else None
            if cached_timings:
                metrics["timings_ms_cached_result"] = dict(cached_timings)
            metrics["timings_ms"] = {
                "cache_lookup_total": cache_elapsed_ms,
            }
        return cached

    indices = get_available_indices()
    served_index = next((i for i in indices if "served-impressions" in i or "served" in i), indices[0])
    viewable_index = next((i for i in indices if "core-view-events" in i), served_index)

    served_filters = _build_base_filters(
        start_date,
        end_date,
        publisher,
        script_id,
        event_types=None,
        gte=gte,
        lt=lt,
    )
    viewable_filters = _build_base_filters(
        start_date,
        end_date,
        publisher,
        script_id,
        event_types=["viewable_impression", "garbage"],
        gte=gte,
        lt=lt,
    )

    # NOTE: served consolidated aggs intentionally skip IP terms.
    # In this environment, IP aggregations can fail for served shards and
    # invalidate the whole consolidated response, forcing expensive per-field fallbacks.
    # IP metrics are still computed later via dedicated/fallback logic.
    served_term_specs = [
        ("country", "geo.country_code", 10),
        ("region", "geo.region_code", 10),
        ("is_mobile", "os.is_mobile", 10),
        ("os", "os.name", 10),
        ("ua_name", "user_agent.name", 10),
    ]
    if ES_INCLUDE_UA_ORIGINAL_AGG:
        served_term_specs.append(("ua_original", "user_agent.original", 10))

    viewable_term_specs = [
        ("source_ip", "source.ip", ES_IP_TERMS_FALLBACK_SIZE),
        ("country", "geo.country_code", 10),
        ("region", "geo.region_code", 10),
        ("resolution", "os.resolution", 10),
        ("device", "device_type", 10),
        ("os", "os.name", 10),
        ("referer", "internal_referer", 10),
        ("ua_name", "user_agent.name", 10),
    ]
    if ES_INCLUDE_UA_ORIGINAL_AGG:
        viewable_term_specs.append(("ua_original", "user_agent.original", 10))
    served_card_specs = [
        ("unique_ip", "source.ip"),
        ("unique_resolution", "os.resolution"),
        ("unique_region", "geo.region_code"),
    ]
    viewable_card_specs = [
        ("unique_ip", "source.ip"),
        ("unique_resolution", "os.resolution"),
        ("unique_region", "geo.region_code"),
    ]

    behavior_hits_limit = max(100, min(ES_BEHAVIOR_BASE_HITS, ES_BEHAVIOR_MAX_HITS))

    served_behavior_query = {
        "size": behavior_hits_limit,
        "track_total_hits": False,
        "_source": [
            "source.ip", "source_ip", "ip",
            "client.ip", "client_ip", "network.client.ip", "request.ip",
            "user_agent.name", "user_agent.original",
            ES_DATE_FIELD,
        ],
        "query": {"bool": {"filter": served_filters}},
    }
    viewable_behavior_query = {
        "size": behavior_hits_limit,
        "track_total_hits": False,
        "_source": [
            "source.ip", "source_ip", "ip",
            "client.ip", "client_ip", "network.client.ip", "request.ip",
            "user_agent.name", "user_agent.original",
            ES_DATE_FIELD,
        ],
        "query": {"bool": {"filter": viewable_filters}},
    }

    served_consolidated_query = _build_consolidated_aggs_query(served_filters, served_term_specs, served_card_specs, include_histogram=True)
    viewable_consolidated_query = _build_consolidated_aggs_query(viewable_filters, viewable_term_specs, viewable_card_specs, include_histogram=True)

    t_queries_start = time_module.perf_counter()
    bundle_responses = _run_msearch_safe([
        (served_index, served_behavior_query),
        (viewable_index, viewable_behavior_query),
        (served_index, served_consolidated_query),
        (viewable_index, viewable_consolidated_query),
    ])
    if bundle_responses is None:
        bundle_responses = _run_searches_parallel([
            (served_index, served_behavior_query),
            (viewable_index, viewable_behavior_query),
            (served_index, served_consolidated_query),
            (viewable_index, viewable_consolidated_query),
        ])

    if not bundle_responses or len(bundle_responses) < 4:
        raise RuntimeError("All suspicious-analysis queries failed due to Elasticsearch/Kibana 400 errors")
    t_queries_end = time_module.perf_counter()

    served_behavior_raw = bundle_responses[0] or {}
    viewable_behavior_raw = bundle_responses[1] or {}
    served_aggs_raw = bundle_responses[2] or {}
    viewable_aggs_raw = bundle_responses[3] or {}

    served_behavior_hits = served_behavior_raw.get("hits", {}).get("hits", []) if served_behavior_raw else []
    viewable_behavior_hits = viewable_behavior_raw.get("hits", {}).get("hits", []) if viewable_behavior_raw else []

    served_docs = _extract_total_hits(served_aggs_raw)
    viewable_docs = _extract_total_hits(viewable_aggs_raw)

    # Aggregation totals can be empty while behavior hits still contain data.
    # In that case, derive counts from behavior responses to avoid zeroed metrics.
    served_behavior_total = _extract_total_hits(served_behavior_raw)
    viewable_behavior_total = _extract_total_hits(viewable_behavior_raw)
    if served_docs <= 0:
        if served_behavior_total > 0:
            served_docs = served_behavior_total
        elif served_behavior_hits:
            served_docs = len(served_behavior_hits)
    if viewable_docs <= 0:
        if viewable_behavior_total > 0:
            viewable_docs = viewable_behavior_total
        elif viewable_behavior_hits:
            viewable_docs = len(viewable_behavior_hits)

    # Adaptive escalation: fetch larger behavior samples only when initial coverage is low.
    if behavior_hits_limit < ES_BEHAVIOR_MAX_HITS:
        served_behavior_coverage = (len(served_behavior_hits) / served_docs) if served_docs > 0 else 1.0
        viewable_behavior_coverage = (len(viewable_behavior_hits) / viewable_docs) if viewable_docs > 0 else 1.0
        need_served_escalation = (
            served_docs > 0
            and served_behavior_coverage < ES_BEHAVIOR_MIN_COVERAGE_FOR_ESCALATION
            and len(served_behavior_hits) < ES_BEHAVIOR_MIN_HITS_FOR_NO_ESCALATION
        )
        need_viewable_escalation = (
            viewable_docs > 0
            and viewable_behavior_coverage < ES_BEHAVIOR_MIN_COVERAGE_FOR_ESCALATION
            and len(viewable_behavior_hits) < ES_BEHAVIOR_MIN_HITS_FOR_NO_ESCALATION
        )

        if need_served_escalation and need_viewable_escalation:
            served_escalation_query = {
                "size": ES_BEHAVIOR_MAX_HITS,
                "track_total_hits": False,
                "_source": [
                    "source.ip", "source_ip", "ip",
                    "client.ip", "client_ip", "network.client.ip", "request.ip",
                    "user_agent.name", "user_agent.original",
                    ES_DATE_FIELD,
                ],
                "query": {"bool": {"filter": served_filters}},
            }
            viewable_escalation_query = {
                "size": ES_BEHAVIOR_MAX_HITS,
                "track_total_hits": False,
                "_source": [
                    "source.ip", "source_ip", "ip",
                    "client.ip", "client_ip", "network.client.ip", "request.ip",
                    "user_agent.name", "user_agent.original",
                    ES_DATE_FIELD,
                ],
                "query": {"bool": {"filter": viewable_filters}},
            }
            escalation_responses = _run_searches_parallel([
                (served_index, served_escalation_query),
                (viewable_index, viewable_escalation_query),
            ])
            served_escalated_raw = escalation_responses[0] if len(escalation_responses) > 0 else None
            viewable_escalated_raw = escalation_responses[1] if len(escalation_responses) > 1 else None

            if served_escalated_raw:
                served_behavior_hits = served_escalated_raw.get("hits", {}).get("hits", [])
            else:
                served_behavior_hits = _fetch_behavior_hits(served_index, served_filters, max_hits=ES_BEHAVIOR_MAX_HITS)
            if viewable_escalated_raw:
                viewable_behavior_hits = viewable_escalated_raw.get("hits", {}).get("hits", [])
            else:
                viewable_behavior_hits = _fetch_behavior_hits(viewable_index, viewable_filters, max_hits=ES_BEHAVIOR_MAX_HITS)

            served_behavior_total = max(served_behavior_total, len(served_behavior_hits))
            viewable_behavior_total = max(viewable_behavior_total, len(viewable_behavior_hits))
        else:
            if need_served_escalation:
                served_behavior_hits = _fetch_behavior_hits(served_index, served_filters, max_hits=ES_BEHAVIOR_MAX_HITS)
                served_behavior_total = max(served_behavior_total, len(served_behavior_hits))
            if need_viewable_escalation:
                viewable_behavior_hits = _fetch_behavior_hits(viewable_index, viewable_filters, max_hits=ES_BEHAVIOR_MAX_HITS)
                viewable_behavior_total = max(viewable_behavior_total, len(viewable_behavior_hits))

    total_docs = served_docs + viewable_docs

    t_compute_start = time_module.perf_counter()
    served_behavior_signals = _compute_behavior_signals_from_hits(served_behavior_hits, max(served_docs, 1))
    viewable_behavior_signals = _compute_behavior_signals_from_hits(viewable_behavior_hits, max(viewable_docs, 1))

    served_ip_top_from_hits = served_behavior_signals["top10_ips"]
    served_ip_event_count_from_hits = int(served_behavior_signals["valid_ip_events"])

    viewable_ip_top_from_hits = viewable_behavior_signals["top10_ips"]
    viewable_ip_event_count_from_hits = int(viewable_behavior_signals["valid_ip_events"])

    # Global-first: prefer aggregation metrics over the full dataset.
    served_ip_top = _extract_terms_from_agg(served_aggs_raw, "source_ip", served_docs)
    served_ip_top = _filter_valid_ip_buckets(served_ip_top)
    served_ip_extraction_mode = "aggregation"

    if served_docs > 0 and not served_ip_top:
        # Fallback to global terms query only if consolidated aggs missed source_ip.
        served_ip_top_raw = _fetch_terms_distribution(served_index, served_filters, "source.ip", served_docs, size=ES_IP_TERMS_FALLBACK_SIZE)
        served_ip_top = _filter_valid_ip_buckets(served_ip_top_raw)
        served_ip_extraction_mode = "terms_query"

    if served_docs > 0 and not served_ip_top and served_ip_top_from_hits:
        # Last resort: sampled behavior hits.
        served_ip_top = served_ip_top_from_hits
        served_ip_extraction_mode = "behavior_hits"

    viewable_ip_top = _extract_terms_from_agg(viewable_aggs_raw, "source_ip", viewable_docs)
    viewable_ip_top = _filter_valid_ip_buckets(viewable_ip_top)
    viewable_ip_extraction_mode = "aggregation"

    if viewable_docs > 0 and not viewable_ip_top:
        viewable_ip_top_raw = _fetch_terms_distribution(viewable_index, viewable_filters, "source.ip", viewable_docs, size=ES_IP_TERMS_FALLBACK_SIZE)
        viewable_ip_top = _filter_valid_ip_buckets(viewable_ip_top_raw)
        viewable_ip_extraction_mode = "terms_query"

    if viewable_docs > 0 and not viewable_ip_top and viewable_ip_top_from_hits:
        viewable_ip_top = viewable_ip_top_from_hits
        viewable_ip_extraction_mode = "behavior_hits"

    # Preserve non-empty top lists when docs fallback to zero in edge cases.
    if served_docs <= 0 and served_ip_top_from_hits:
        served_ip_top = served_ip_top_from_hits
        served_ip_extraction_mode = "behavior_hits"
    if viewable_docs <= 0 and viewable_ip_top_from_hits:
        viewable_ip_top = viewable_ip_top_from_hits
        viewable_ip_extraction_mode = "behavior_hits"

    served_aggs_raw = served_aggs_raw or {}
    viewable_aggs_raw = viewable_aggs_raw or {}

    def _terms_with_fallback(
        raw: dict,
        agg_name: str,
        index: str,
        filters: list[dict],
        field: str,
        total: int,
        size: int = 10,
        allow_fallback: bool = True,
    ) -> list[dict]:
        terms = _extract_terms_from_agg(raw, agg_name, total)
        if terms or total <= 0:
            return terms
        if not allow_fallback:
            return []
        return _fetch_terms_distribution(index, filters, field, total, size=size)

    def _cardinality_with_fallback(raw: dict, agg_name: str, index: str, filters: list[dict], field: str) -> int:
        value = _extract_cardinality_from_agg(raw, agg_name)
        if value > 0:
            return value
        return _fetch_unique_count(index, filters, field)

    served_country = _terms_with_fallback(served_aggs_raw, "country", served_index, served_filters, "geo.country_code", served_docs)
    viewable_country = _terms_with_fallback(viewable_aggs_raw, "country", viewable_index, viewable_filters, "geo.country_code", viewable_docs)

    served_region = [
        x for x in _terms_with_fallback(served_aggs_raw, "region", served_index, served_filters, "geo.region_code", served_docs)
        if x["value"] != "__EMPTY__"
    ]
    viewable_region = [
        x for x in _terms_with_fallback(viewable_aggs_raw, "region", viewable_index, viewable_filters, "geo.region_code", viewable_docs)
        if x["value"] != "__EMPTY__"
    ]
    served_is_mobile = _terms_with_fallback(served_aggs_raw, "is_mobile", served_index, served_filters, "os.is_mobile", served_docs)
    served_os = _terms_with_fallback(served_aggs_raw, "os", served_index, served_filters, "os.name", served_docs)
    served_ua_name = _terms_with_fallback(served_aggs_raw, "ua_name", served_index, served_filters, "user_agent.name", served_docs)
    if not served_ua_name:
        served_ua_name = list(served_behavior_signals.get("top10_ua_name", []))

    served_ua_original = list(served_behavior_signals.get("top10_ua_original", []))
    if ES_INCLUDE_UA_ORIGINAL_AGG:
        agg_served_ua_original = _terms_with_fallback(
            served_aggs_raw,
            "ua_original",
            served_index,
            served_filters,
            "user_agent.original",
            served_docs,
            allow_fallback=False,
        )
        if agg_served_ua_original:
            served_ua_original = agg_served_ua_original

    viewable_resolution = [
        x for x in _terms_with_fallback(viewable_aggs_raw, "resolution", viewable_index, viewable_filters, "os.resolution", viewable_docs)
        if x["value"] != "__EMPTY__"
    ]
    viewable_device = _terms_with_fallback(viewable_aggs_raw, "device", viewable_index, viewable_filters, "device_type", viewable_docs)
    viewable_os = _terms_with_fallback(viewable_aggs_raw, "os", viewable_index, viewable_filters, "os.name", viewable_docs)
    viewable_referer = _terms_with_fallback(viewable_aggs_raw, "referer", viewable_index, viewable_filters, "internal_referer", viewable_docs)
    viewable_ua_name = _terms_with_fallback(viewable_aggs_raw, "ua_name", viewable_index, viewable_filters, "user_agent.name", viewable_docs)
    if not viewable_ua_name:
        viewable_ua_name = list(viewable_behavior_signals.get("top10_ua_name", []))

    viewable_ua_original = list(viewable_behavior_signals.get("top10_ua_original", []))
    if ES_INCLUDE_UA_ORIGINAL_AGG:
        agg_viewable_ua_original = _terms_with_fallback(
            viewable_aggs_raw,
            "ua_original",
            viewable_index,
            viewable_filters,
            "user_agent.original",
            viewable_docs,
            allow_fallback=False,
        )
        if agg_viewable_ua_original:
            viewable_ua_original = agg_viewable_ua_original

    ua_merged = _merge_top_counts(served_ua_original, viewable_ua_original)
    if not ua_merged:
        ua_merged = _merge_top_counts(served_ua_name, viewable_ua_name)

    suspicious_ua_pattern = re.compile(r"bot|crawler|spider|headless|phantom|selenium|python|curl|wget|scrapy", re.IGNORECASE)
    suspicious_ua_count = sum(item["count"] for item in ua_merged if suspicious_ua_pattern.search(item["value"] or ""))
    suspicious_ua_count = max(
        suspicious_ua_count,
        int(served_behavior_signals.get("suspicious_ua_event_count", 0) or 0)
        + int(viewable_behavior_signals.get("suspicious_ua_event_count", 0) or 0),
    )

    unique_ip_served = _extract_cardinality_from_agg(served_aggs_raw, "unique_ip")
    unique_ip_viewable = _extract_cardinality_from_agg(viewable_aggs_raw, "unique_ip")
    unique_ip_served_source = "cardinality" if unique_ip_served > 0 else "unavailable"
    unique_ip_viewable_source = "cardinality" if unique_ip_viewable > 0 else "unavailable"

    # If cardinality is unavailable, keep explicit estimate from behavior-hit sample.
    served_unique_ip_estimate = unique_ip_served
    if served_unique_ip_estimate <= 0:
        served_unique_ip_estimate = int(served_behavior_signals.get("unique_ip_count", 0) or 0)
        if served_unique_ip_estimate > 0:
            unique_ip_served_source = "behavior_hits_estimate"

    viewable_unique_ip_estimate = unique_ip_viewable
    if viewable_unique_ip_estimate <= 0:
        viewable_unique_ip_estimate = int(viewable_behavior_signals.get("unique_ip_count", 0) or 0)
        if viewable_unique_ip_estimate > 0:
            unique_ip_viewable_source = "behavior_hits_estimate"
    
    # CRITICAL: If we have unique_ip count but no top IP buckets, fetch them now
    if unique_ip_served > 0 and not served_ip_top:
        served_ip_top_raw = _fetch_terms_distribution(served_index, served_filters, "source.ip", served_docs, size=ES_IP_TERMS_FALLBACK_SIZE)
        served_ip_top = _filter_valid_ip_buckets(served_ip_top_raw)
        served_ip_extraction_mode = "critical_fallback_served"
    
    if unique_ip_viewable > 0 and not viewable_ip_top:
        viewable_ip_top_raw = _fetch_terms_distribution(viewable_index, viewable_filters, "source.ip", viewable_docs, size=ES_IP_TERMS_FALLBACK_SIZE)
        viewable_ip_top = _filter_valid_ip_buckets(viewable_ip_top_raw)
        viewable_ip_extraction_mode = "critical_fallback_viewable"
    unique_resolution_served = _cardinality_with_fallback(served_aggs_raw, "unique_resolution", served_index, served_filters, "os.resolution")
    unique_resolution_viewable = _cardinality_with_fallback(viewable_aggs_raw, "unique_resolution", viewable_index, viewable_filters, "os.resolution")
    unique_region_served = _cardinality_with_fallback(served_aggs_raw, "unique_region", served_index, served_filters, "geo.region_code")
    unique_region_viewable = _cardinality_with_fallback(viewable_aggs_raw, "unique_region", viewable_index, viewable_filters, "geo.region_code")

    served_hist_counts = _extract_histogram_from_agg(served_aggs_raw)
    viewable_hist_counts = _extract_histogram_from_agg(viewable_aggs_raw)
    if not served_hist_counts and served_docs > 0:
        served_hist_counts = _fetch_histogram_counts(served_index, served_filters, timeout_seconds=5.0)
        if not served_hist_counts:
            logger.warning("Histogram fallback returned empty for served index=%s", served_index)
    if not viewable_hist_counts and viewable_docs > 0:
        viewable_hist_counts = _fetch_histogram_counts(viewable_index, viewable_filters, timeout_seconds=5.0)
        if not viewable_hist_counts:
            logger.warning("Histogram fallback returned empty for viewable index=%s", viewable_index)
    combined_histogram = served_hist_counts[:]
    if viewable_hist_counts and len(viewable_hist_counts) == len(combined_histogram):
        combined_histogram = [combined_histogram[i] + viewable_hist_counts[i] for i in range(len(combined_histogram))]
    elif not combined_histogram:
        combined_histogram = viewable_hist_counts

    # Keep denominators consistent with the source used for unique IPs.
    served_events_base_for_user = served_docs
    viewable_events_base_for_user = viewable_docs
    served_avg_events_per_user = round(served_events_base_for_user / served_unique_ip_estimate, 2) if served_unique_ip_estimate > 0 else None
    viewable_avg_events_per_user = round(viewable_events_base_for_user / viewable_unique_ip_estimate, 2) if viewable_unique_ip_estimate > 0 else None
    served_avg_seconds_per_event = float(served_behavior_signals["avg_seconds_between_events"])
    viewable_avg_seconds_per_event = float(viewable_behavior_signals["avg_seconds_between_events"])
    
    # Reuse precomputed cadence metrics from single-pass behavior summaries.
    served_avg_seconds_per_user_event = float(served_behavior_signals["avg_seconds_between_events_per_user"])
    viewable_avg_seconds_per_user_event = float(viewable_behavior_signals["avg_seconds_between_events_per_user"])
    uniformity_cv = _compute_uniformity_score(combined_histogram)

    # Use internally extracted totals for consistency in suspicious metrics.
    viewability_served_ratio = round((viewable_docs / served_docs), 4) if served_docs > 0 else 0.0

    served_events_base_for_ip_pct = served_docs
    viewable_events_base_for_ip_pct = viewable_docs
    if served_ip_extraction_mode == "behavior_hits" and served_ip_event_count_from_hits > 0:
        served_events_base_for_ip_pct = served_ip_event_count_from_hits
    if viewable_ip_extraction_mode == "behavior_hits" and viewable_ip_event_count_from_hits > 0:
        viewable_events_base_for_ip_pct = viewable_ip_event_count_from_hits

    served_same_ip_pct = _percent(int(served_ip_top[0]["count"]), served_events_base_for_ip_pct) if served_ip_top else 0.0
    viewable_same_ip_pct = _percent(int(viewable_ip_top[0]["count"]), viewable_events_base_for_ip_pct) if viewable_ip_top else 0.0
    served_top10_ip_pct = _top_n_pct(served_ip_top, served_events_base_for_ip_pct, 10)
    viewable_top10_ip_pct = _top_n_pct(viewable_ip_top, viewable_events_base_for_ip_pct, 10)
    # IP metrics available if we have top buckets OR unique IP count (fallback success)
    served_ip_metrics_available = (served_docs <= 0) or bool(served_ip_top) or (unique_ip_served > 0)
    viewable_ip_metrics_available = (viewable_docs <= 0) or bool(viewable_ip_top) or (unique_ip_viewable > 0)
    served_events_base_for_repetition = served_docs
    viewable_events_base_for_repetition = viewable_docs

    served_ip_repetition_pct = _repeated_ip_traffic_pct(served_ip_top, served_events_base_for_repetition)
    viewable_ip_repetition_pct = _repeated_ip_traffic_pct(viewable_ip_top, viewable_events_base_for_repetition)

    served_ip_scope = "global" if served_ip_extraction_mode in ("aggregation", "terms_query", "critical_fallback_served") else "sample"
    viewable_ip_scope = "global" if viewable_ip_extraction_mode in ("aggregation", "terms_query", "critical_fallback_viewable") else "sample"
    served_unique_scope = "global" if unique_ip_served_source == "cardinality" else "sample"
    viewable_unique_scope = "global" if unique_ip_viewable_source == "cardinality" else "sample"

    served_doc_coverage_pct = 100.0 if served_ip_scope == "global" else _percent(served_ip_event_count_from_hits, served_docs)
    viewable_doc_coverage_pct = 100.0 if viewable_ip_scope == "global" else _percent(viewable_ip_event_count_from_hits, viewable_docs)

    served_confidence = round(
        (1.0 if served_ip_scope == "global" else max(0.2, served_doc_coverage_pct / 100.0)) * 0.6
        + (1.0 if served_unique_scope == "global" else max(0.2, served_doc_coverage_pct / 100.0)) * 0.4,
        3,
    )
    viewable_confidence = round(
        (1.0 if viewable_ip_scope == "global" else max(0.2, viewable_doc_coverage_pct / 100.0)) * 0.6
        + (1.0 if viewable_unique_scope == "global" else max(0.2, viewable_doc_coverage_pct / 100.0)) * 0.4,
        3,
    )

    total_docs_for_conf = max(served_docs + viewable_docs, 1)
    overall_confidence = round(
        (
            served_confidence * served_docs
            + viewable_confidence * viewable_docs
        ) / total_docs_for_conf,
        3,
    )

    # Stage 3: confidence by signal category.
    ip_confidence = round((served_confidence + viewable_confidence) / 2.0, 3)

    served_temporal_coverage_pct = _percent(len(served_behavior_hits), served_docs) if served_docs > 0 else 100.0
    viewable_temporal_coverage_pct = _percent(len(viewable_behavior_hits), viewable_docs) if viewable_docs > 0 else 100.0
    temporal_confidence = round(
        (
            max(0.2, served_temporal_coverage_pct / 100.0) * served_docs
            + max(0.2, viewable_temporal_coverage_pct / 100.0) * viewable_docs
        ) / total_docs_for_conf,
        3,
    )

    distribution_confidence = round(
        min(1.0, max(overall_confidence, 0.85 if (served_docs + viewable_docs) > 0 else overall_confidence)),
        3,
    )

    weighted_analysis_confidence = round(
        (ip_confidence * 0.45) + (temporal_confidence * 0.35) + (distribution_confidence * 0.20),
        3,
    )

    served_daily_user_volume = _extract_daily_volume_from_agg(served_aggs_raw, "ip_daily_volume")
    viewable_daily_user_volume = _extract_daily_volume_from_agg(viewable_aggs_raw, "ip_daily_volume")
    served_daily_volume_source = "agg"
    viewable_daily_volume_source = "agg"
    if _is_daily_volume_empty(served_daily_user_volume):
        served_daily_user_volume = dict(served_behavior_signals["daily_user_volume"])
        served_daily_volume_source = "behavior_hits_fallback"
    if _is_daily_volume_empty(viewable_daily_user_volume):
        viewable_daily_user_volume = dict(viewable_behavior_signals["daily_user_volume"])
        viewable_daily_volume_source = "behavior_hits_fallback"

    combined_daily_by_user_day = _extract_ip_daily_counts_from_agg(served_aggs_raw, "ip_daily_volume")
    for key, count in _extract_ip_daily_counts_from_agg(viewable_aggs_raw, "ip_daily_volume").items():
        combined_daily_by_user_day[key] = combined_daily_by_user_day.get(key, 0) + count
    combined_daily_volume_source = "agg_merged"
    if not combined_daily_by_user_day:
        combined_hits_by_user_day = _build_user_day_counts_from_hits(served_behavior_hits)
        for key, count in _build_user_day_counts_from_hits(viewable_behavior_hits).items():
            combined_hits_by_user_day[key] = combined_hits_by_user_day.get(key, 0) + count
        combined_daily_by_user_day = combined_hits_by_user_day
        combined_daily_volume_source = "behavior_hits_merged_fallback"
    combined_daily_user_volume = _compute_daily_volume_from_user_day(combined_daily_by_user_day)
    
    top_country_pct = max(
        _percent(int(served_country[0]["count"]), served_docs) if served_country else 0.0,
        _percent(int(viewable_country[0]["count"]), viewable_docs) if viewable_country else 0.0,
    )
    top_region_pct = max(
        _percent(int(served_region[0]["count"]), served_docs) if served_region else 0.0,
        _percent(int(viewable_region[0]["count"]), viewable_docs) if viewable_region else 0.0,
    )
    top_resolution_pct = _percent(viewable_resolution[0]["count"], viewable_docs) if viewable_resolution else 0.0
    suspicious_ua_pct = _percent(suspicious_ua_count, total_docs)
    unique_ip_total = unique_ip_served + unique_ip_viewable
    ip_diversity_pct = _percent(unique_ip_viewable, viewable_docs) if viewable_docs > 0 else 0.0
    unique_resolution_total = unique_resolution_served + unique_resolution_viewable
    resolution_diversity_pct = _percent(unique_resolution_total, total_docs)
    unique_region_total = unique_region_served + unique_region_viewable

    device_top_pct = 0.0
    if viewable_device:
        device_top_pct = max(_percent(int(item.get("count", 0) or 0), viewable_docs) for item in viewable_device)

    score = 0
    flags: list[str] = []

    available_top10_candidates = [v for v, ok in [
        (served_top10_ip_pct, served_ip_metrics_available),
        (viewable_top10_ip_pct, viewable_ip_metrics_available),
    ] if ok]
    available_same_ip_candidates = [v for v, ok in [
        (served_same_ip_pct, served_ip_metrics_available),
        (viewable_same_ip_pct, viewable_ip_metrics_available),
    ] if ok]
    available_ip_repetition_candidates = [v for v, ok in [
        (served_ip_repetition_pct, served_ip_metrics_available),
        (viewable_ip_repetition_pct, viewable_ip_metrics_available),
    ] if ok and v is not None]

    strongest_top10_ip_pct = max(available_top10_candidates) if available_top10_candidates else 0.0
    strongest_same_ip_pct = max(available_same_ip_candidates) if available_same_ip_candidates else 0.0
    strongest_ip_repetition_pct = max(available_ip_repetition_candidates) if available_ip_repetition_candidates else None
    cadence_values = [v for v in (served_avg_seconds_per_event, viewable_avg_seconds_per_event) if v > 0]
    min_avg_seconds_per_event = min(cadence_values) if cadence_values else 0.0
    cadence_available = bool(cadence_values)
    cadence_source = "hits" if cadence_available else "unavailable"

    if strongest_top10_ip_pct >= 95:
        score += 20
        flags.append("Concentração muito alta no top 10 de IPs")
    elif strongest_top10_ip_pct >= 85:
        score += 12

    if strongest_same_ip_pct >= 25:
        score += 20
        flags.append("Concentração extrema em um único IP")
    elif strongest_same_ip_pct >= 15:
        score += 12

    over20_records = combined_daily_user_volume["records_over_20"]
    over50_records = combined_daily_user_volume["records_over_50"]
    over100_records = combined_daily_user_volume["records_over_100"]
    repeat_over20_users = combined_daily_user_volume["repeat_users_over_20_days"]
    repeat_over50_users = combined_daily_user_volume["repeat_users_over_50_days"]
    combined_weighted_excess = combined_daily_user_volume["weighted_excess"]
    max_events_single_user_day = max(
        served_daily_user_volume["max_events_single_user_day"],
        viewable_daily_user_volume["max_events_single_user_day"],
        combined_daily_user_volume["max_events_single_user_day"],
    )

    if over20_records > 0:
        score += min(16, over20_records * 2)
    if over50_records > 0:
        score += min(20, over50_records * 3)
        flags.append("Usuários com volume diário acima de 50 eventos")
    if over100_records > 0:
        score += min(25, over100_records * 4)
        flags.append("Usuários com volume diário acima de 100 eventos")

    if repeat_over20_users > 0:
        score += min(16, repeat_over20_users * 4)
        flags.append("Padrão recorrente: usuários repetindo volume alto em múltiplos dias")
    if repeat_over50_users > 0:
        score += min(12, repeat_over50_users * 4)

    if combined_weighted_excess >= 300:
        score += 10
    elif combined_weighted_excess >= 120:
        score += 6

    if max_events_single_user_day >= 3000:
        score += 70
        flags.append("Pico crítico: usuário com 3000+ eventos em um dia")
    elif max_events_single_user_day >= 1500:
        score += 52
        flags.append("Pico extremo: usuário com 1500+ eventos em um dia")
    elif max_events_single_user_day >= 700:
        score += 35
        flags.append("Pico muito alto: usuário com 700+ eventos em um dia")
    elif max_events_single_user_day >= 250:
        score += 20
        flags.append("Pico elevado: usuário com 250+ eventos em um dia")

    if combined_weighted_excess >= 6000:
        score += 24
    elif combined_weighted_excess >= 2500:
        score += 16
    elif combined_weighted_excess >= 1000:
        score += 10

    if top_country_pct >= 95 and unique_ip_total <= 5:
        score += 10
        flags.append("Concentração geográfica extrema com poucos IPs")

    if top_region_pct >= 85:
        score += 18
        flags.append("Concentração anômala em uma única região")
    elif top_region_pct >= 70:
        score += 10

    if top_resolution_pct >= 70:
        score += 15
        flags.append("Padronização suspeita de resolução de tela")
    elif top_resolution_pct >= 55:
        score += 8

    if unique_resolution_total >= 120 and resolution_diversity_pct >= 6.0:
        score += 14
        flags.append("Diversidade incomum de resoluções de tela")
    elif unique_resolution_total >= 80 and resolution_diversity_pct >= 4.0:
        score += 8

    strongest_user_event_gap = min(
        v for v in [served_avg_seconds_per_user_event, viewable_avg_seconds_per_user_event] if v > 0
    ) if (served_avg_seconds_per_user_event > 0 or viewable_avg_seconds_per_user_event > 0) else 0.0

    if strongest_user_event_gap > 0 and strongest_user_event_gap < 1.0:
        score += 22
        flags.append("Tempo médio entre eventos do mesmo usuário extremamente baixo")
    elif strongest_user_event_gap > 0 and strongest_user_event_gap < 2.0:
        score += 12

    if ip_diversity_pct >= 60 and (strongest_user_event_gap > 0 and strongest_user_event_gap < 2.5):
        score += 18
        flags.append("Padrão distribuído: muitos IPs com cadência curta")
    elif ip_diversity_pct >= 45 and cadence_available and min_avg_seconds_per_event < 2.0:
        score += 10

    if cadence_available:
        if min_avg_seconds_per_event < 1.0:
            score += 15
            flags.append("Cadência de eventos rápida demais para tráfego orgânico")
        elif min_avg_seconds_per_event < 2.0:
            score += 8

    if (
        uniformity_cv > 0
        and uniformity_cv < 0.35
        and (strongest_same_ip_pct >= 15 or max_events_single_user_day >= 250)
    ):
        score += 10
        flags.append("Distribuição temporal excessivamente uniforme")

    if suspicious_ua_pct >= 10:
        score += 15
        flags.append("Volume relevante de user agents suspeitos")
    elif suspicious_ua_pct >= 3:
        score += 8

    if device_top_pct >= 90:
        score += 8

    if served_docs > 0 and (viewability_served_ratio > 1.1 or viewability_served_ratio < 0.05):
        score += 12
        flags.append("Relação served vs viewable fora de faixa esperada")

    if served_docs > 0 and not served_ip_top:
        flags.append("Telemetria de IP ausente ou inválida em Served")
    if viewable_docs > 0 and not viewable_ip_top:
        flags.append("Telemetria de IP ausente ou inválida em Viewable")

    critical_signal_count = 0
    if strongest_same_ip_pct >= 25:
        critical_signal_count += 1
    if strongest_top10_ip_pct >= 95:
        critical_signal_count += 1
    if max_events_single_user_day >= 700:
        critical_signal_count += 1
    if combined_weighted_excess >= 2500:
        critical_signal_count += 1
    if strongest_user_event_gap > 0 and strongest_user_event_gap < 1.2:
        critical_signal_count += 1
    if top_region_pct >= 85:
        critical_signal_count += 1
    if unique_resolution_total >= 120 and resolution_diversity_pct >= 6.0:
        critical_signal_count += 1
    if suspicious_ua_pct >= 10:
        critical_signal_count += 1
    if repeat_over20_users >= 3:
        critical_signal_count += 1

    if critical_signal_count >= 6:
        score += 22
        flags.append("Múltiplos sinais críticos simultâneos")
    elif critical_signal_count >= 4:
        score += 12

    # Hard floor for obviously non-organic behavior in extreme burst scenarios.
    should_apply_hard_floor = max_events_single_user_day >= 3000 and (
        strongest_user_event_gap < 2.0
        or strongest_same_ip_pct >= 20
        or combined_weighted_excess >= 2500
        or repeat_over20_users >= 1
    )

    t_compute_end = time_module.perf_counter()
    no_docs = served_docs == 0 and viewable_docs == 0
    raw_score = 0.0 if no_docs else min(score, 100)
    confidence_multiplier = 0.0 if no_docs else round(0.3 + (0.7 * weighted_analysis_confidence), 3)
    score = 0.0 if no_docs else round(min(100.0, raw_score * confidence_multiplier), 2)
    if should_apply_hard_floor:
        confidence_aware_floor = round(95 * confidence_multiplier, 2)
        score = max(score, min(100.0, confidence_aware_floor))

    if score >= 85:
        risk_level = "Risco extremamente alto de tráfego inválido"
    elif score >= 60:
        risk_level = "Alto risco de tráfego inválido"
    elif score >= 40:
        risk_level = "Risco moderado de tráfego suspeito"
    else:
        risk_level = "Baixo risco de tráfego inválido"

    t_total_end = time_module.perf_counter()
    result = {
        "enabled": True,
        "risk_score": score,
        "risk_score_raw": raw_score,
        "confidence_multiplier": confidence_multiplier,
        "risk_level": risk_level,
        "flags": flags,
        "overall": {
            "total_docs": total_docs,
            "analysis_confidence": overall_confidence,
            "analysis_confidence_weighted": weighted_analysis_confidence,
            "ip_confidence": ip_confidence,
            "temporal_confidence": temporal_confidence,
            "distribution_confidence": distribution_confidence,
            "served_temporal_coverage_pct": served_temporal_coverage_pct,
            "viewable_temporal_coverage_pct": viewable_temporal_coverage_pct,
            "risk_score_raw": raw_score,
            "risk_score_adjusted": score,
            "confidence_multiplier": confidence_multiplier,
            "served_doc_coverage_pct": served_doc_coverage_pct,
            "viewable_doc_coverage_pct": viewable_doc_coverage_pct,
            "top10_ip_concentration_pct": strongest_top10_ip_pct,
            "ip_repetition_pct": strongest_ip_repetition_pct,
            "same_ip_traffic_pct": strongest_same_ip_pct,
            "ip_metrics_available": bool(available_same_ip_candidates),
            "ip_diversity_pct": ip_diversity_pct,
            "suspicious_user_agent_pct": suspicious_ua_pct,
            "viewability_served_ratio": viewability_served_ratio,
            "time_uniformity_cv": uniformity_cv,
            "top_country_pct": top_country_pct,
            "top_region_pct": top_region_pct,
            "unique_region_total": unique_region_total,
            "resolution_diversity_pct": resolution_diversity_pct,
            "unique_resolution_total": unique_resolution_total,
            "records_over_20_per_day": over20_records,
            "records_over_50_per_day": over50_records,
            "records_over_100_per_day": over100_records,
            "repeat_users_over_20_days": repeat_over20_users,
            "repeat_users_over_50_days": repeat_over50_users,
            "daily_weighted_excess": combined_weighted_excess,
            "max_events_single_user_day": max_events_single_user_day,
            "critical_signal_count": critical_signal_count,
        },
        "served_analysis": {
            "doc_count": served_docs,
            "top_10_ips": served_ip_top,
            "ip_extraction_mode": served_ip_extraction_mode,
            "ip_metric_scope": served_ip_scope,
            "unique_ip_source": unique_ip_served_source,
            "doc_coverage_pct": served_doc_coverage_pct,
            "analysis_confidence": served_confidence,
            "sample_size_limit": ES_BEHAVIOR_MAX_HITS if served_ip_extraction_mode == "behavior_hits" else ES_IP_TERMS_FALLBACK_SIZE,
            "same_ip_traffic_pct": served_same_ip_pct if served_ip_metrics_available else None,
            "top10_ip_concentration_pct": served_top10_ip_pct if served_ip_metrics_available else None,
            "ip_repetition_pct": served_ip_repetition_pct,
            "ip_metrics_available": served_ip_metrics_available,
            "country_distribution": served_country,
            "region_distribution": served_region,
            "top_region_pct": _percent(int(served_region[0]["count"]), served_docs) if served_region else 0.0,
            "is_mobile_distribution": served_is_mobile,
            "os_distribution": served_os,
            "user_agent_name_distribution": served_ua_name,
            "user_agent_original_distribution": served_ua_original,
            "avg_events_per_user": served_avg_events_per_user,
            "avg_seconds_between_events": served_avg_seconds_per_event,
            "avg_seconds_between_events_per_user": served_avg_seconds_per_user_event,
            "unique_users_estimate": served_unique_ip_estimate if unique_ip_served_source != "unavailable" else None,
            "daily_user_volume": served_daily_user_volume,
            "daily_volume_source": served_daily_volume_source,
            "max_events_single_user_day": served_daily_user_volume["max_events_single_user_day"],
        },
        "viewable_analysis": {
            "doc_count": viewable_docs,
            "top_10_ips": viewable_ip_top,
            "ip_extraction_mode": viewable_ip_extraction_mode,
            "ip_metric_scope": viewable_ip_scope,
            "unique_ip_source": unique_ip_viewable_source,
            "doc_coverage_pct": viewable_doc_coverage_pct,
            "analysis_confidence": viewable_confidence,
            "sample_size_limit": ES_BEHAVIOR_MAX_HITS if viewable_ip_extraction_mode == "behavior_hits" else ES_IP_TERMS_FALLBACK_SIZE,
            "same_ip_traffic_pct": viewable_same_ip_pct if viewable_ip_metrics_available else None,
            "top10_ip_concentration_pct": viewable_top10_ip_pct if viewable_ip_metrics_available else None,
            "ip_repetition_pct": viewable_ip_repetition_pct,
            "ip_metrics_available": viewable_ip_metrics_available,
            "country_distribution": viewable_country,
            "region_distribution": viewable_region,
            "top_region_pct": _percent(int(viewable_region[0]["count"]), viewable_docs) if viewable_region else 0.0,
            "device_type_distribution": viewable_device,
            "internal_referer_distribution": viewable_referer,
            "os_distribution": viewable_os,
            "resolution_distribution": viewable_resolution,
            "user_agent_name_distribution": viewable_ua_name,
            "user_agent_original_distribution": viewable_ua_original,
            "avg_events_per_user": viewable_avg_events_per_user,
            "avg_seconds_between_events": viewable_avg_seconds_per_event,
            "avg_seconds_between_events_per_user": viewable_avg_seconds_per_user_event,
            "unique_users_estimate": viewable_unique_ip_estimate if unique_ip_viewable_source != "unavailable" else None,
            "daily_user_volume": viewable_daily_user_volume,
            "daily_volume_source": viewable_daily_volume_source,
            "max_events_single_user_day": viewable_daily_user_volume["max_events_single_user_day"],
        },
        "metrics": {
            "served_docs": served_docs,
            "viewable_docs": viewable_docs,
            "total_docs": total_docs,
            "analysis_confidence": overall_confidence,
            "analysis_confidence_weighted": weighted_analysis_confidence,
            "ip_confidence": ip_confidence,
            "temporal_confidence": temporal_confidence,
            "distribution_confidence": distribution_confidence,
            "served_temporal_coverage_pct": served_temporal_coverage_pct,
            "viewable_temporal_coverage_pct": viewable_temporal_coverage_pct,
            "risk_score_raw": raw_score,
            "risk_score_adjusted": score,
            "confidence_multiplier": confidence_multiplier,
            "served_doc_coverage_pct": served_doc_coverage_pct,
            "viewable_doc_coverage_pct": viewable_doc_coverage_pct,
            "cache_hit": False,
            "timings_ms": {
                "query_bundle": round((t_queries_end - t_queries_start) * 1000, 1),
                "compute_and_score": round((t_compute_end - t_compute_start) * 1000, 1),
                "total": round((t_total_end - analysis_started_at) * 1000, 1),
            },
            "cadence_source": cadence_source,
            # Indicates whether daily volume signals came from global agg or behavior-hit fallback.
            "daily_volume_source": {
                "served": served_daily_volume_source,
                "viewable": viewable_daily_volume_source,
                "combined": combined_daily_volume_source,
            },
            "sample_note": f"Served: {served_docs} registros consultados (amostra: 500 via agregação ou {ES_IP_FALLBACK_MAX_HITS} via fallback). Viewable: {viewable_docs} registros consultados (amostra: 500 via agregação).",
            "top_10_ips_served": served_ip_top,
            "top_10_ips_viewable": viewable_ip_top,
            "top_10_ips_merged": _merge_top_counts(served_ip_top, viewable_ip_top),
            "ip_repetition_pct": strongest_ip_repetition_pct,
            "same_ip_traffic_pct": strongest_same_ip_pct,
            "country_distribution": _merge_top_counts(served_country, viewable_country),
            "region_distribution": served_region,
            "resolution_distribution": viewable_resolution,
            "avg_events_per_user": round(total_docs / (served_unique_ip_estimate + viewable_unique_ip_estimate), 2) if (served_unique_ip_estimate + viewable_unique_ip_estimate) > 0 else None,
            "avg_seconds_between_events": round(window_seconds / max(total_docs, 1), 3),
            "time_uniformity_cv": uniformity_cv,
            "suspicious_user_agent_pct": suspicious_ua_pct,
            "viewability_served_ratio": viewability_served_ratio,
            "device_type_distribution": viewable_device,
        },
    }

    _cache_set_suspicious(cache_key, result)
    return result
