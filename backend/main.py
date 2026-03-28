"""
main.py — FastAPI backend for the AdOps Discrepancy Analysis Tool.
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from parser import parse_publisher_report
from elastic import (
    query_elasticsearch,
    get_available_indices,
    get_index_fields,
    get_field_top_values,
)
from analysis import calculate_discrepancy
from ai_explainer import generate_explanation

load_dotenv()

app = FastAPI(title="AdOps Discrepancy Analyzer", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend static files
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# ── Request / Response Models ────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    report_text: str
    publisher: str | None = None


class AnalyzeResponse(BaseModel):
    # Dates
    start_date: str
    end_date: str
    publisher: str

    # Publisher metrics
    pub_served: int
    pub_viewable: int
    pub_viewability_pct: float

    # Clever metrics
    clever_served: int
    clever_viewable: int
    clever_viewability_pct: float

    # Discrepancy
    diff_served: int
    diff_viewable: int
    viewability_diff_pp: float
    viewability_diff_pct: float
    status: str

    # AI explanation
    explanation: str


class IndexField(BaseModel):
    name: str
    type: str


class IndexFieldsResponse(BaseModel):
    fields: list[IndexField]


class FieldValue(BaseModel):
    value: str
    count: int
    percent: float


class FieldValuesResponse(BaseModel):
    values: list[FieldValue]
    total_docs: int


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/")
async def serve_frontend():
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "AdOps Discrepancy Analyzer API is running. Open frontend/index.html in a browser."}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/indices")
async def list_indices():
    """Returns list of available Elasticsearch indices."""
    try:
        indices = get_available_indices()
        return {"indices": indices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list indices: {str(e)}")


@app.get("/indices/{index}/fields", response_model=IndexFieldsResponse)
async def list_index_fields(index: str):
    """Returns list of available fields for a given index."""
    try:
        return get_index_fields(index)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch index fields: {str(e)}")


@app.get("/indices/{index}/field/{field}/values", response_model=FieldValuesResponse)
async def list_field_values(index: str, field: str, size: int = 5):
    """Returns top N values for a specific field with frequencies."""
    try:
        return get_field_top_values(index, field, size=size)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch field values: {str(e)}")


@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze(request: AnalyzeRequest):
    """
    Main endpoint: receives a raw publisher report, runs full analysis pipeline.
    """
    # Validate input
    if not request.report_text or not request.report_text.strip():
        raise HTTPException(
            status_code=422,
            detail="Report text is empty. Please paste the publisher report."
        )
    
    # 1. Parse publisher report with Gemini
    try:
        parsed = parse_publisher_report(request.report_text)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse report with AI: {str(e)}")

    # Validate required fields
    missing = [f for f in ("served_impressions", "start_date", "end_date") if not parsed.get(f)]
    if missing:
        extracted_fields = {k: v for k, v in parsed.items() if v is not None}
        raise HTTPException(
            status_code=422,
            detail=f"Could not extract required fields from report: {', '.join(missing)}. "
                   f"Report must contain impression counts and date range. "
                   f"(Extracted: {extracted_fields if extracted_fields else 'nothing'})",
        )

    pub_served   = parsed["served_impressions"]
    pub_viewable = parsed.get("viewable_impressions") or 0
    start_date   = parsed["start_date"]
    end_date     = parsed["end_date"]
    publisher    = request.publisher or ""

    # 2. Query Elasticsearch for internal data (using first available index by default)
    try:
        index = get_available_indices()[0]
        clever_data = query_elasticsearch(index, start_date, end_date, publisher or None)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch query failed: {str(e)}")

    clever_served   = clever_data["served_impressions"]
    clever_viewable = clever_data["viewable_impressions"]

    # 3. Calculate discrepancies
    result = calculate_discrepancy(pub_served, pub_viewable, clever_served, clever_viewable)

    # 4. Generate AI explanation
    try:
        explanation = generate_explanation(result, start_date, end_date, publisher or "Not specified")
    except Exception as e:
        explanation = f"AI explanation unavailable: {str(e)}"

    return AnalyzeResponse(
        start_date=start_date,
        end_date=end_date,
        publisher=publisher or "Not specified",
        pub_served=result.pub_served,
        pub_viewable=result.pub_viewable,
        pub_viewability_pct=round(result.pub_viewability * 100, 2),
        clever_served=result.clever_served,
        clever_viewable=result.clever_viewable,
        clever_viewability_pct=round(result.clever_viewability * 100, 2),
        diff_served=result.diff_served,
        diff_viewable=result.diff_viewable,
        viewability_diff_pp=round(result.viewability_diff_pp, 2),
        viewability_diff_pct=round(result.viewability_diff_pct, 2),
        status=result.status,
        explanation=explanation,
    )
