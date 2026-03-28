"""
elastic.py — Queries Elasticsearch for internal ad server data (Clever).
All index names and field names are configurable via environment variables.
"""

import os
import httpx
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

ES_URL      = os.environ.get("ES_URL", "http://localhost:9200").strip()
# Ensure URL has protocol
if ES_URL and not ES_URL.startswith(("http://", "https://")):
    ES_URL = f"https://{ES_URL}"

ES_USERNAME = os.environ.get("ES_USERNAME", "elastic")
ES_PASSWORD = os.environ.get("ES_PASSWORD", "")
ES_INDICES  = os.environ.get("ES_INDICES", "served-impressions,core-view-events").split(",")

# Field name mappings — override via .env as needed
ES_DATE_FIELD      = os.environ.get("ES_DATE_FIELD", "@timestamp")
ES_PUBLISHER_FIELD = os.environ.get("ES_PUBLISHER_FIELD", "publisher")
ES_SERVED_FIELD    = os.environ.get("ES_SERVED_FIELD", "served_impressions")
ES_VIEWABLE_FIELD  = os.environ.get("ES_VIEWABLE_FIELD", "viewable_impressions")

# HTTP client setup
def _get_auth():
    """Returns auth tuple if credentials are provided."""
    return (ES_USERNAME, ES_PASSWORD) if ES_PASSWORD else None

def _get_client():
    """Returns a configured HTTP client."""
    return httpx.Client(verify=False, timeout=30.0)




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
        response = client.get(
            f"{ES_URL}/{index}/_mapping",
            auth=auth,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    
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
        response = client.post(
            f"{ES_URL}/{index}/_search",
            json=query,
            auth=auth,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    
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
    must_clauses = [
        {
            "range": {
                ES_DATE_FIELD: {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "yyyy-MM-dd",
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


def _build_query(start_date: str, end_date: str, publisher: str | None) -> dict:
    """Build an Elasticsearch aggregation query."""
    must_clauses = [
        {
            "range": {
                ES_DATE_FIELD: {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "yyyy-MM-dd",
                }
            }
        }
    ]

    if publisher:
        must_clauses.append(
            {
                "bool": {
                    "should": [
                        {"term": {f"{ES_PUBLISHER_FIELD}.keyword": publisher}},
                        {"wildcard": {f"{ES_PUBLISHER_FIELD}.keyword": f"*{publisher}*"}},
                        {"match": {ES_PUBLISHER_FIELD: publisher}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )

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
) -> dict:
    """
    Queries Elasticsearch and returns aggregated served and viewable impressions.
    
    Args:
        index: Index name to query
        start_date: Start date (yyyy-MM-dd)
        end_date: End date (yyyy-MM-dd)
        publisher: Optional publisher filter

    Returns:
        {
            "served_impressions": int,
            "viewable_impressions": int,
            "doc_count": int,
        }
    Raises httpx.HTTPStatusError on bad responses.
    """
    query = _build_query(start_date, end_date, publisher)

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

    aggs = data.get("aggregations", {})
    served   = int(aggs.get("total_served",   {}).get("value", 0) or 0)
    viewable = int(aggs.get("total_viewable", {}).get("value", 0) or 0)
    doc_count = data.get("hits", {}).get("total", {}).get("value", 0)

    return {
        "served_impressions": served,
        "viewable_impressions": viewable,
        "doc_count": doc_count,
    }
