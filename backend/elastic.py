"""
elastic.py — Queries Elasticsearch for internal ad server data (Clever).
All index names and field names are configurable via environment variables.
"""

import os
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
