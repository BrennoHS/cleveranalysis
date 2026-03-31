"""
elastic.py — Queries Elasticsearch for internal ad server data (Clever).
All index names and field names are configurable via environment variables.
"""

import os
import re
import ipaddress
import httpx
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

try:
    ES_QUERY_TZ = ZoneInfo(ES_TIMEZONE)
except Exception:
    ES_QUERY_TZ = timezone.utc


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

def _get_client():
    """Returns a configured HTTP client."""
    return httpx.Client(verify=False, timeout=30.0)


def _is_kibana_url() -> bool:
    return "kibana" in ES_URL.lower()


def _kibana_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
        "kbn-version": KIBANA_VERSION,
    }


def _kibana_search(index: str, body: dict) -> dict:
    auth = _get_auth()
    payload = {"params": {"index": index, "body": body}}

    with _get_client() as client:
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

    with _get_client() as client:
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
    
    with _get_client() as client:
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
    
    with _get_client() as client:
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
    
    # Add field filters
    if filters:
        for field_name, filter_spec in filters.items():
            agg_field = f"{field_name}.keyword" if not field_name.startswith("@") else field_name
            
            if filter_spec.get("must_include"):
                must_clauses.append({
                    "terms": {agg_field: filter_spec["must_include"]}
                })
    
    must_not_clauses = []
    if filters:
        for field_name, filter_spec in filters.items():
            agg_field = f"{field_name}.keyword" if not field_name.startswith("@") else field_name
            
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

    with _get_client() as client:
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


def _run_search(index: str, body: dict) -> dict:
    """Run search either via Kibana internal API or direct Elasticsearch."""
    if _is_kibana_url():
        return _kibana_search(index, body)

    auth = _get_auth()
    with _get_client() as client:
        response = client.post(
            f"{ES_URL}/{index}/_search",
            json=body,
            auth=auth,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    return response.json()


def _run_search_safe(index: str, body: dict) -> dict | None:
    """Run search and return None on HTTP errors (used for best-effort suspicious analysis)."""
    try:
        return _run_search(index, body)
    except httpx.HTTPStatusError:
        return None


def _extract_terms(raw: dict, agg_name: str, total_docs: int) -> list[dict]:
    buckets = raw.get("aggregations", {}).get(agg_name, {}).get("buckets", [])
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
) -> list[dict]:
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
        ES_DATE_FIELD,
    }
    if field in exact_fields:
        return field

    # Default for text-ish dimensions.
    return f"{field}.keyword"


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
        aggs[_field_to_agg_name(f)] = {
            "terms": {
                "field": _agg_field_for_terms(f),
                "size": 10,
                "missing": "__EMPTY__",
            }
        }

    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": aggs,
    }


def _build_terms_query(filters: list[dict], field: str, size: int = 10) -> dict:
    agg_name = _field_to_agg_name(field)
    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            agg_name: {
                "terms": {
                    "field": _agg_field_for_terms(field),
                    "size": size,
                    "missing": "__EMPTY__",
                }
            }
        },
    }


def _build_cardinality_query(filters: list[dict], field: str) -> dict:
    return {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            "unique_value": {
                "cardinality": {
                    "field": _agg_field_for_terms(field),
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


def _fetch_terms_distribution(index: str, filters: list[dict], field: str, total_docs: int) -> list[dict]:
    raw = _run_search_safe(index, _build_terms_query(filters, field, size=10))
    if not raw:
        return []
    return _extract_terms(raw, _field_to_agg_name(field), total_docs)


def _fetch_unique_count(index: str, filters: list[dict], field: str) -> int:
    raw = _run_search_safe(index, _build_cardinality_query(filters, field))
    if not raw:
        return 0
    return int(raw.get("aggregations", {}).get("unique_value", {}).get("value", 0) or 0)


def _fetch_histogram_counts(index: str, filters: list[dict]) -> list[int]:
    raw = _run_search_safe(index, _build_histogram_query(filters))
    if not raw:
        return []
    buckets = raw.get("aggregations", {}).get("per_minute", {}).get("buckets", [])
    return [int(b.get("doc_count", 0) or 0) for b in buckets]


def _parse_event_timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def _compute_avg_seconds_between_events_per_user(index: str, filters: list[dict], max_hits: int = 12000) -> float:
    query = {
        "size": max_hits,
        "track_total_hits": False,
        "_source": ["source.ip", ES_DATE_FIELD],
        "query": {"bool": {"filter": filters}},
        "sort": [{ES_DATE_FIELD: {"order": "asc"}}],
    }
    raw = _run_search_safe(index, query)
    if not raw:
        return 0.0

    hits = raw.get("hits", {}).get("hits", [])
    if not hits:
        return 0.0

    prev_by_ip: dict[str, datetime] = {}
    deltas: list[float] = []

    for hit in hits:
        src = hit.get("_source", {})
        ip = _extract_source_ip(src)
        ts_val = src.get(ES_DATE_FIELD)
        if not ip:
            continue
        ts = _parse_event_timestamp(ts_val)
        if ts is None:
            continue

        if ip in prev_by_ip:
            delta = (ts - prev_by_ip[ip]).total_seconds()
            if delta >= 0:
                deltas.append(delta)
        prev_by_ip[ip] = ts

    if not deltas:
        return 0.0
    return round(sum(deltas) / len(deltas), 3)


def _compute_daily_user_volume_signals(index: str, filters: list[dict], max_hits: int = 20000) -> dict:
    query = {
        "size": max_hits,
        "track_total_hits": False,
        "_source": ["source.ip", ES_DATE_FIELD],
        "query": {"bool": {"filter": filters}},
        "sort": [{ES_DATE_FIELD: {"order": "asc"}}],
    }
    raw = _run_search_safe(index, query)
    if not raw:
        return {
            "records_over_20": 0,
            "records_over_50": 0,
            "records_over_100": 0,
            "repeat_users_over_20_days": 0,
            "repeat_users_over_50_days": 0,
            "max_events_single_user_day": 0,
            "weighted_excess": 0,
        }

    hits = raw.get("hits", {}).get("hits", [])
    if not hits:
        return {
            "records_over_20": 0,
            "records_over_50": 0,
            "records_over_100": 0,
            "repeat_users_over_20_days": 0,
            "repeat_users_over_50_days": 0,
            "max_events_single_user_day": 0,
            "weighted_excess": 0,
        }

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

    if not by_user_day:
        return {
            "records_over_20": 0,
            "records_over_50": 0,
            "records_over_100": 0,
            "repeat_users_over_20_days": 0,
            "repeat_users_over_50_days": 0,
            "max_events_single_user_day": 0,
            "weighted_excess": 0,
        }

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
        src.get("ip"),
        src.get("client", {}).get("ip") if isinstance(src.get("client"), dict) else None,
        src.get("client.ip"),
        src.get("network", {}).get("client", {}).get("ip") if isinstance(src.get("network"), dict) else None,
    ]
    for candidate in candidates:
        normalized = _normalize_ip_value(candidate)
        if _is_valid_ip(normalized):
            return normalized
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
    """
    indices = get_available_indices()
    served_index = next((i for i in indices if "served-impressions" in i or "served" in i), indices[0])
    viewable_index = next((i for i in indices if "core-view-events" in i), served_index)

    served_filters = _build_base_filters(
        start_date,
        end_date,
        publisher,
        script_id,
        event_types=None,
    )
    viewable_filters = _build_base_filters(
        start_date,
        end_date,
        publisher,
        script_id,
        event_types=["viewable_impression", "garbage"],
    )

    served_count_raw = _run_search_safe(
        served_index,
        {
            "size": 0,
            "track_total_hits": True,
            "query": {"bool": {"filter": served_filters}},
        },
    )
    viewable_count_raw = _run_search_safe(
        viewable_index,
        {
            "size": 0,
            "track_total_hits": True,
            "query": {"bool": {"filter": viewable_filters}},
        },
    )

    if served_count_raw is None and viewable_count_raw is None:
        raise RuntimeError("All suspicious-analysis queries failed due to Elasticsearch/Kibana 400 errors")

    served_docs = _extract_total_hits(served_count_raw or {})
    viewable_docs = _extract_total_hits(viewable_count_raw or {})

    total_docs = served_docs + viewable_docs

    served_ip_top_raw = _fetch_terms_distribution(served_index, served_filters, "source.ip", served_docs)
    viewable_ip_top_raw = _fetch_terms_distribution(viewable_index, viewable_filters, "source.ip", viewable_docs)
    served_ip_top = _filter_valid_ip_buckets(served_ip_top_raw)
    viewable_ip_top = _filter_valid_ip_buckets(viewable_ip_top_raw)
    served_country = _fetch_terms_distribution(served_index, served_filters, "geo.country_code", served_docs)
    viewable_country = _fetch_terms_distribution(viewable_index, viewable_filters, "geo.country_code", viewable_docs)

    served_region = [
        x for x in _fetch_terms_distribution(served_index, served_filters, "geo.region_code", served_docs)
        if x["value"] != "__EMPTY__"
    ]
    viewable_region = [
        x for x in _fetch_terms_distribution(viewable_index, viewable_filters, "geo.region_code", viewable_docs)
        if x["value"] != "__EMPTY__"
    ]
    served_is_mobile = _fetch_terms_distribution(served_index, served_filters, "os.is_mobile", served_docs)
    served_os = _fetch_terms_distribution(served_index, served_filters, "os.name", served_docs)
    served_ua_name = _fetch_terms_distribution(served_index, served_filters, "user_agent.name", served_docs)
    served_ua_original = _fetch_terms_distribution(served_index, served_filters, "user_agent.original", served_docs)

    viewable_resolution = [
        x for x in _fetch_terms_distribution(viewable_index, viewable_filters, "os.resolution", viewable_docs)
        if x["value"] != "__EMPTY__"
    ]
    viewable_device = _fetch_terms_distribution(viewable_index, viewable_filters, "device_type", viewable_docs)
    viewable_os = _fetch_terms_distribution(viewable_index, viewable_filters, "os.name", viewable_docs)
    viewable_referer = _fetch_terms_distribution(viewable_index, viewable_filters, "internal_referer", viewable_docs)
    viewable_ua_name = _fetch_terms_distribution(viewable_index, viewable_filters, "user_agent.name", viewable_docs)
    viewable_ua_original = _fetch_terms_distribution(viewable_index, viewable_filters, "user_agent.original", viewable_docs)

    ua_merged = _merge_top_counts(served_ua_original, viewable_ua_original)

    suspicious_ua_pattern = re.compile(r"bot|crawler|spider|headless|phantom|selenium|python|curl|wget|scrapy", re.IGNORECASE)
    suspicious_ua_count = sum(item["count"] for item in ua_merged if suspicious_ua_pattern.search(item["value"] or ""))

    unique_ip_served = _fetch_unique_count(served_index, served_filters, "source.ip")
    unique_ip_viewable = _fetch_unique_count(viewable_index, viewable_filters, "source.ip")
    unique_resolution_served = _fetch_unique_count(served_index, served_filters, "os.resolution")
    unique_resolution_viewable = _fetch_unique_count(viewable_index, viewable_filters, "os.resolution")
    unique_region_served = _fetch_unique_count(served_index, served_filters, "geo.region_code")
    unique_region_viewable = _fetch_unique_count(viewable_index, viewable_filters, "geo.region_code")

    served_hist_counts = _fetch_histogram_counts(served_index, served_filters)
    viewable_hist_counts = _fetch_histogram_counts(viewable_index, viewable_filters)
    combined_histogram = served_hist_counts[:]
    if viewable_hist_counts and len(viewable_hist_counts) == len(combined_histogram):
        combined_histogram = [combined_histogram[i] + viewable_hist_counts[i] for i in range(len(combined_histogram))]
    elif not combined_histogram:
        combined_histogram = viewable_hist_counts

    start_dt = _parse_range_input(start_date, is_end=False)
    end_dt = _parse_range_input(end_date, is_end=True)
    window_seconds = max(int((end_dt - start_dt).total_seconds()), 1)

    served_avg_events_per_user = round(served_docs / max(unique_ip_served, 1), 2)
    viewable_avg_events_per_user = round(viewable_docs / max(unique_ip_viewable, 1), 2)
    served_avg_seconds_per_event = round(window_seconds / max(served_docs, 1), 3)
    viewable_avg_seconds_per_event = round(window_seconds / max(viewable_docs, 1), 3)
    served_avg_seconds_per_user_event = _compute_avg_seconds_between_events_per_user(served_index, served_filters)
    viewable_avg_seconds_per_user_event = _compute_avg_seconds_between_events_per_user(viewable_index, viewable_filters)
    uniformity_cv = _compute_uniformity_score(combined_histogram)

    viewable_adjusted = viewable_impressions + garbage_impressions
    viewability_served_ratio = round((viewable_adjusted / served_impressions), 4) if served_impressions > 0 else 0.0

    served_same_ip_pct = _percent(int(served_ip_top[0]["count"]), served_docs) if served_ip_top else 0.0
    viewable_same_ip_pct = _percent(int(viewable_ip_top[0]["count"]), viewable_docs) if viewable_ip_top else 0.0
    served_top10_ip_pct = _top_n_pct(served_ip_top, served_docs, 10)
    viewable_top10_ip_pct = _top_n_pct(viewable_ip_top, viewable_docs, 10)

    served_daily_user_volume = _compute_daily_user_volume_signals(served_index, served_filters)
    viewable_daily_user_volume = _compute_daily_user_volume_signals(viewable_index, viewable_filters)

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
    ip_diversity_pct = _percent(unique_ip_total, total_docs)
    unique_resolution_total = unique_resolution_served + unique_resolution_viewable
    resolution_diversity_pct = _percent(unique_resolution_total, total_docs)
    unique_region_total = unique_region_served + unique_region_viewable

    device_top_pct = 0.0
    if viewable_device:
        device_top_pct = max(_percent(int(item.get("count", 0) or 0), viewable_docs) for item in viewable_device)

    score = 0
    flags: list[str] = []

    strongest_top10_ip_pct = max(served_top10_ip_pct, viewable_top10_ip_pct)
    strongest_same_ip_pct = max(served_same_ip_pct, viewable_same_ip_pct)

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

    over20_records = served_daily_user_volume["records_over_20"] + viewable_daily_user_volume["records_over_20"]
    over50_records = served_daily_user_volume["records_over_50"] + viewable_daily_user_volume["records_over_50"]
    over100_records = served_daily_user_volume["records_over_100"] + viewable_daily_user_volume["records_over_100"]
    repeat_over20_users = (
        served_daily_user_volume["repeat_users_over_20_days"] + viewable_daily_user_volume["repeat_users_over_20_days"]
    )
    repeat_over50_users = (
        served_daily_user_volume["repeat_users_over_50_days"] + viewable_daily_user_volume["repeat_users_over_50_days"]
    )
    combined_weighted_excess = served_daily_user_volume["weighted_excess"] + viewable_daily_user_volume["weighted_excess"]
    max_events_single_user_day = max(
        served_daily_user_volume["max_events_single_user_day"],
        viewable_daily_user_volume["max_events_single_user_day"],
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

    if top_country_pct >= 95:
        flags.append("Concentração geográfica por país elevada (contextual)")

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
    elif ip_diversity_pct >= 45 and min(served_avg_seconds_per_event, viewable_avg_seconds_per_event) < 2.0:
        score += 10

    if min(served_avg_seconds_per_event, viewable_avg_seconds_per_event) < 1.0:
        score += 15
        flags.append("Cadência de eventos rápida demais para tráfego orgânico")
    elif min(served_avg_seconds_per_event, viewable_avg_seconds_per_event) < 2.0:
        score += 8

    if uniformity_cv > 0 and uniformity_cv < 0.35:
        score += 10
        flags.append("Distribuição temporal excessivamente uniforme")

    if suspicious_ua_pct >= 10:
        score += 15
        flags.append("Volume relevante de user agents suspeitos")
    elif suspicious_ua_pct >= 3:
        score += 8

    if device_top_pct >= 90:
        score += 8

    if served_impressions > 0 and (viewability_served_ratio > 1.1 or viewability_served_ratio < 0.05):
        score += 12
        flags.append("Relação served vs viewable/garbage fora de faixa esperada")

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
    if max_events_single_user_day >= 3000 and (
        strongest_user_event_gap < 2.0
        or strongest_same_ip_pct >= 20
        or combined_weighted_excess >= 2500
        or repeat_over20_users >= 1
    ):
        score = max(score, 95)

    score = min(score, 100)
    if score >= 85:
        risk_level = "Risco extremamente alto de tráfego inválido"
    elif score >= 60:
        risk_level = "Alto risco de tráfego inválido"
    elif score >= 40:
        risk_level = "Risco moderado de tráfego suspeito"
    else:
        risk_level = "Baixo risco de tráfego inválido"

    return {
        "enabled": True,
        "risk_score": score,
        "risk_level": risk_level,
        "flags": flags,
        "overall": {
            "total_docs": total_docs,
            "top10_ip_concentration_pct": strongest_top10_ip_pct,
            "same_ip_traffic_pct": strongest_same_ip_pct,
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
            "same_ip_traffic_pct": served_same_ip_pct,
            "top10_ip_concentration_pct": served_top10_ip_pct,
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
            "unique_users_estimate": unique_ip_served,
            "daily_user_volume": served_daily_user_volume,
            "max_events_single_user_day": served_daily_user_volume["max_events_single_user_day"],
        },
        "viewable_analysis": {
            "doc_count": viewable_docs,
            "top_10_ips": viewable_ip_top,
            "same_ip_traffic_pct": viewable_same_ip_pct,
            "top10_ip_concentration_pct": viewable_top10_ip_pct,
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
            "unique_users_estimate": unique_ip_viewable,
            "daily_user_volume": viewable_daily_user_volume,
            "max_events_single_user_day": viewable_daily_user_volume["max_events_single_user_day"],
        },
        "metrics": {
            "served_docs": served_docs,
            "viewable_docs": viewable_docs,
            "total_docs": total_docs,
            "top_10_ips": _merge_top_counts(served_ip_top, viewable_ip_top),
            "same_ip_traffic_pct": strongest_same_ip_pct,
            "country_distribution": _merge_top_counts(served_country, viewable_country),
            "region_distribution": served_region,
            "resolution_distribution": viewable_resolution,
            "avg_events_per_user": round(total_docs / max(unique_ip_served + unique_ip_viewable, 1), 2),
            "avg_seconds_between_events": round(window_seconds / max(total_docs, 1), 3),
            "time_uniformity_cv": uniformity_cv,
            "suspicious_user_agent_pct": suspicious_ua_pct,
            "viewability_served_ratio": viewability_served_ratio,
            "device_type_distribution": viewable_device,
        },
    }
