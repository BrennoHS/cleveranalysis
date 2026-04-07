"""
main.py — FastAPI backend for the AdOps Discrepancy Analysis Tool.
"""

import os
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from parser import parse_publisher_report
from report_file_parser import parse_publisher_report_file
from elastic import (
    query_elasticsearch,
    analyze_suspicious_traffic,
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
    start_date: str
    end_date: str
    publisher: str | None = None
    script_id: str | None = None
    suspicious_traffic: bool = False


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
    clever_garbage: int
    clever_viewability_pct: float

    # Discrepancy
    diff_served: int
    diff_viewable: int
    served_discrepancy_pct: float
    viewable_discrepancy_pct: float
    viewability_diff_pp: float
    viewability_diff_pct: float
    status: str

    # AI explanation
    explanation: str

    # Optional suspicious traffic analysis
    suspicious_analysis: dict | None = None


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


def _run_analysis_pipeline(
    parsed: dict,
    publisher: str | None,
    script_id: str | None,
    start_date_override: str,
    end_date_override: str,
    suspicious_traffic: bool = False,
) -> AnalyzeResponse:
    script_id_safe = (script_id or "").strip()
    if not script_id_safe:
        raise HTTPException(
            status_code=422,
            detail="Script ID é obrigatório para prosseguir com a análise."
        )

    effective = dict(parsed)
    effective["start_date"] = start_date_override.strip()
    effective["end_date"] = end_date_override.strip()

    missing = [f for f in ("served_impressions", "start_date", "end_date") if not effective.get(f)]
    if missing:
        extracted_fields = {k: v for k, v in effective.items() if v is not None}
        raise HTTPException(
            status_code=422,
            detail=f"Could not extract required fields from report: {', '.join(missing)}. "
                   f"Report must contain impression counts and date range. "
                   f"(Extracted: {extracted_fields if extracted_fields else 'nothing'})",
        )

    pub_served = effective["served_impressions"]
    pub_viewable = effective.get("viewable_impressions") or 0
    start_date = effective["start_date"]
    end_date = effective["end_date"]
    publisher_safe = publisher or ""
    script_id_safe = script_id_safe or None

    try:
        index = get_available_indices()[0]
        clever_data = query_elasticsearch(index, start_date, end_date, publisher_safe or None, script_id_safe)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch query failed: {str(e)}")

    clever_served = clever_data["served_impressions"]
    clever_viewable = clever_data["viewable_impressions"]
    clever_garbage = clever_data.get("garbage_impressions", 0)

    result = calculate_discrepancy(pub_served, pub_viewable, clever_served, clever_viewable, clever_garbage)

    suspicious_analysis: dict | None = None
    if suspicious_traffic:
        try:
            suspicious_analysis = analyze_suspicious_traffic(
                start_date=start_date,
                end_date=end_date,
                publisher=publisher_safe or None,
                script_id=script_id_safe,
                served_impressions=clever_served,
                viewable_impressions=clever_viewable,
                garbage_impressions=clever_garbage,
            )
        except Exception as e:
            suspicious_analysis = {
                "enabled": True,
                "error": f"Failed to run suspicious traffic analysis: {str(e)}",
            }

    try:
        explanation = generate_explanation(
            result,
            start_date,
            end_date,
            publisher_safe or "Not specified",
            suspicious_analysis=suspicious_analysis,
        )
    except Exception as e:
        explanation = f"Análise da IA indisponível no momento: {str(e)}"

    return AnalyzeResponse(
        start_date=start_date,
        end_date=end_date,
        publisher=publisher_safe or "Not specified",
        pub_served=result.pub_served,
        pub_viewable=result.pub_viewable,
        pub_viewability_pct=round(result.pub_viewability * 100, 2),
        clever_served=result.clever_served,
        clever_viewable=result.clever_viewable,
        clever_garbage=clever_garbage,
        clever_viewability_pct=round(result.clever_viewability * 100, 2),
        diff_served=result.diff_served,
        diff_viewable=result.diff_viewable,
        served_discrepancy_pct=round(result.served_discrepancy_pct, 2),
        viewable_discrepancy_pct=round(result.viewable_discrepancy_pct, 2),
        viewability_diff_pp=round(result.viewability_diff_pp, 2),
        viewability_diff_pct=round(result.viewability_diff_pct, 2),
        status=result.status,
        explanation=explanation,
        suspicious_analysis=suspicious_analysis,
    )


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
    
    # 1. Parse publisher report (deterministic parser with optional AI fallback)
    try:
        parsed = parse_publisher_report(request.report_text)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse report: {str(e)}")

    return _run_analysis_pipeline(
        parsed,
        request.publisher,
        request.script_id,
        start_date_override=request.start_date,
        end_date_override=request.end_date,
        suspicious_traffic=request.suspicious_traffic,
    )


@app.post("/analyze-file", response_model=AnalyzeResponse)
async def analyze_file(
    file: UploadFile = File(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    publisher: str | None = Form(None),
    script_id: str | None = Form(None),
    suspicious_traffic: bool = Form(False),
):
    """
    Analyze endpoint for structured report files (.xlsx/.csv) with a date range.
    """
    content = await file.read()
    if not content:
        raise HTTPException(status_code=422, detail="Uploaded report file is empty.")

    try:
        parsed = parse_publisher_report_file(content, file.filename or "", start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse uploaded report: {str(e)}")

    return _run_analysis_pipeline(
        parsed,
        publisher,
        script_id,
        start_date_override=start_date,
        end_date_override=end_date,
        suspicious_traffic=suspicious_traffic,
    )
