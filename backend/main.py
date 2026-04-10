"""
main.py — FastAPI backend for the AdOps Discrepancy Analysis Tool.
"""

import os
import io
import csv
import json
import uuid
import threading
from datetime import datetime
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
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


class AnalyzeJobResponse(BaseModel):
    job_id: str


HISTORY_FILE = os.path.join(os.path.dirname(__file__), "analysis_history.json")
HISTORY_MAX_ITEMS = 100

_jobs_lock = threading.Lock()
_history_lock = threading.Lock()
_analysis_jobs: dict[str, dict] = {}
_analysis_history: list[dict] = []
_latest_analysis_id: str | None = None


def _load_history() -> None:
    global _analysis_history, _latest_analysis_id
    if not os.path.exists(HISTORY_FILE):
        _analysis_history = []
        _latest_analysis_id = None
        return

    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            _analysis_history = data[-HISTORY_MAX_ITEMS:]
            _latest_analysis_id = _analysis_history[-1]["id"] if _analysis_history else None
        else:
            _analysis_history = []
            _latest_analysis_id = None
    except Exception:
        _analysis_history = []
        _latest_analysis_id = None


def _save_history() -> None:
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(_analysis_history[-HISTORY_MAX_ITEMS:], f, ensure_ascii=False, indent=2)


def _analysis_to_dict(result: AnalyzeResponse) -> dict:
    if hasattr(result, "model_dump"):
        return result.model_dump()
    return result.dict()


def _append_history(result: AnalyzeResponse, script_id: str | None) -> str:
    global _latest_analysis_id
    result_dict = _analysis_to_dict(result)
    entry = {
        "id": uuid.uuid4().hex,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "publisher": result_dict.get("publisher") or "não especificado",
        "script_id": (script_id or "").strip() or None,
        "start_date": result_dict.get("start_date"),
        "end_date": result_dict.get("end_date"),
        "status": result_dict.get("status"),
        "result": result_dict,
    }
    with _history_lock:
        _analysis_history.append(entry)
        if len(_analysis_history) > HISTORY_MAX_ITEMS:
            _analysis_history[:] = _analysis_history[-HISTORY_MAX_ITEMS:]
        _latest_analysis_id = entry["id"]
        _save_history()
    return entry["id"]


def _validate_date_range_or_422(start_date_str: str, end_date_str: str) -> None:
    try:
        start_dt = datetime.strptime(start_date_str.strip(), "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date_str.strip(), "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(
            status_code=422,
            detail="Datas inválidas. Use o formato YYYY-MM-DD para data inicial e final.",
        )

    if start_dt > end_dt:
        raise HTTPException(
            status_code=422,
            detail="Intervalo inválido: a data inicial deve ser menor ou igual à data final.",
        )

    range_days = (end_dt - start_dt).days + 1
    if range_days > 90:
        raise HTTPException(
            status_code=422,
            detail="Intervalo inválido: o período máximo permitido é de 90 dias.",
        )


def _set_job_status(job_id: str, state: str, message: str, result: dict | None = None, error: str | None = None) -> None:
    with _jobs_lock:
        current = _analysis_jobs.get(job_id, {})
        current.update(
            {
                "job_id": job_id,
                "state": state,
                "message": message,
            }
        )
        if result is not None:
            current["result"] = result
        if error is not None:
            current["error"] = error
        _analysis_jobs[job_id] = current


def _run_analysis_pipeline(
    parsed: dict,
    publisher: str | None,
    script_id: str | None,
    start_date_override: str,
    end_date_override: str,
    suspicious_traffic: bool = False,
    progress_callback=None,
) -> AnalyzeResponse:
    _validate_date_range_or_422(start_date_override, end_date_override)

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
        if progress_callback:
            progress_callback("Query principal ao Elasticsearch")
        index = get_available_indices()[0]
        clever_data = query_elasticsearch(index, start_date, end_date, publisher_safe or None, script_id_safe)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha na consulta ao Elasticsearch: {str(e)}")

    clever_served = clever_data["served_impressions"]
    clever_viewable = clever_data["viewable_impressions"]
    clever_garbage = clever_data.get("garbage_impressions", 0)

    result = calculate_discrepancy(pub_served, pub_viewable, clever_served, clever_viewable, clever_garbage)

    suspicious_analysis: dict | None = None
    if suspicious_traffic:
        try:
            if progress_callback:
                progress_callback("Análise de tráfego suspeito")
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
                "error": f"Falha ao executar análise de tráfego suspeito: {str(e)}",
            }

    try:
        if progress_callback:
            progress_callback("Geração da explicação com IA")
        explanation = generate_explanation(
            result,
            start_date,
            end_date,
            publisher_safe or "não especificado",
            suspicious_analysis=suspicious_analysis,
        )
    except Exception as e:
        explanation = "Análise da IA indisponível no momento."

    response = AnalyzeResponse(
        start_date=start_date,
        end_date=end_date,
        publisher=publisher_safe or "não especificado",
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
    _append_history(response, script_id_safe)
    return response


def _run_text_job(job_id: str, request: AnalyzeRequest) -> None:
    try:
        _set_job_status(job_id, "running", "Parsing do relatório")
        parsed = parse_publisher_report(request.report_text)

        def progress(msg: str) -> None:
            _set_job_status(job_id, "running", msg)

        response = _run_analysis_pipeline(
            parsed,
            request.publisher,
            request.script_id,
            start_date_override=request.start_date,
            end_date_override=request.end_date,
            suspicious_traffic=request.suspicious_traffic,
            progress_callback=progress,
        )
        _set_job_status(job_id, "completed", "Análise finalizada", result=_analysis_to_dict(response))
    except HTTPException as exc:
        _set_job_status(job_id, "failed", "Falha na análise", error=str(exc.detail))
    except Exception:
        _set_job_status(job_id, "failed", "Falha na análise", error="Erro interno ao processar a análise.")


def _run_file_job(
    job_id: str,
    content: bytes,
    filename: str,
    start_date: str,
    end_date: str,
    publisher: str | None,
    script_id: str | None,
    suspicious_traffic: bool,
) -> None:
    try:
        _set_job_status(job_id, "running", "Parsing do relatório")
        parsed = parse_publisher_report_file(content, filename, start_date, end_date)

        def progress(msg: str) -> None:
            _set_job_status(job_id, "running", msg)

        response = _run_analysis_pipeline(
            parsed,
            publisher,
            script_id,
            start_date_override=start_date,
            end_date_override=end_date,
            suspicious_traffic=suspicious_traffic,
            progress_callback=progress,
        )
        _set_job_status(job_id, "completed", "Análise finalizada", result=_analysis_to_dict(response))
    except HTTPException as exc:
        _set_job_status(job_id, "failed", "Falha na análise", error=str(exc.detail))
    except Exception:
        _set_job_status(job_id, "failed", "Falha na análise", error="Erro interno ao processar a análise.")


def _get_history_entry_by_id(entry_id: str) -> dict | None:
    with _history_lock:
        for item in _analysis_history:
            if item.get("id") == entry_id:
                return item
    return None


def _flatten_for_csv(entry: dict) -> dict:
    result = entry.get("result", {})
    suspicious = result.get("suspicious_analysis") or {}
    overall = suspicious.get("overall") or {}
    metrics = suspicious.get("metrics") or {}

    return {
        "id": entry.get("id"),
        "timestamp": entry.get("timestamp"),
        "publisher": entry.get("publisher"),
        "script_id": entry.get("script_id") or "",
        "start_date": result.get("start_date"),
        "end_date": result.get("end_date"),
        "status": result.get("status"),
        "pub_served": result.get("pub_served"),
        "pub_viewable": result.get("pub_viewable"),
        "pub_viewability_pct": result.get("pub_viewability_pct"),
        "clever_served": result.get("clever_served"),
        "clever_viewable": result.get("clever_viewable"),
        "clever_garbage": result.get("clever_garbage"),
        "clever_viewability_pct": result.get("clever_viewability_pct"),
        "served_discrepancy_pct": result.get("served_discrepancy_pct"),
        "viewable_discrepancy_pct": result.get("viewable_discrepancy_pct"),
        "viewability_diff_pp": result.get("viewability_diff_pp"),
        "suspicious_enabled": suspicious.get("enabled"),
        "suspicious_risk_score": suspicious.get("risk_score"),
        "suspicious_risk_level": suspicious.get("risk_level"),
        "suspicious_same_ip_pct": overall.get("same_ip_traffic_pct", metrics.get("same_ip_traffic_pct")),
        "suspicious_ip_repetition_pct": overall.get("ip_repetition_pct", metrics.get("ip_repetition_pct")),
        "suspicious_top10_ip_pct": overall.get("top10_ip_concentration_pct", metrics.get("top10_ip_concentration_pct")),
        "suspicious_uniformity_cv": overall.get("time_uniformity_cv", metrics.get("time_uniformity_cv")),
    }


_load_history()


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
            detail="O texto do relatório está vazio. Cole o relatório do publisher para analisar."
        )
    
    # 1. Parse publisher report (deterministic parser with optional AI fallback)
    try:
        parsed = parse_publisher_report(request.report_text)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Falha ao interpretar o relatório: {str(e)}")

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
        raise HTTPException(status_code=422, detail="O arquivo enviado está vazio.")

    try:
        parsed = parse_publisher_report_file(content, file.filename or "", start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Falha ao interpretar o relatório enviado: {str(e)}")

    return _run_analysis_pipeline(
        parsed,
        publisher,
        script_id,
        start_date_override=start_date,
        end_date_override=end_date,
        suspicious_traffic=suspicious_traffic,
    )


@app.post("/analyze-job", response_model=AnalyzeJobResponse)
async def analyze_job(request: AnalyzeRequest):
    if not request.report_text or not request.report_text.strip():
        raise HTTPException(
            status_code=422,
            detail="O texto do relatório está vazio. Cole o relatório do publisher para analisar.",
        )

    job_id = uuid.uuid4().hex
    _set_job_status(job_id, "queued", "Aguardando processamento")
    worker = threading.Thread(target=_run_text_job, args=(job_id, request), daemon=True)
    worker.start()
    return AnalyzeJobResponse(job_id=job_id)


@app.post("/analyze-file-job", response_model=AnalyzeJobResponse)
async def analyze_file_job(
    file: UploadFile = File(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    publisher: str | None = Form(None),
    script_id: str | None = Form(None),
    suspicious_traffic: bool = Form(False),
):
    content = await file.read()
    if not content:
        raise HTTPException(status_code=422, detail="O arquivo enviado está vazio.")

    job_id = uuid.uuid4().hex
    _set_job_status(job_id, "queued", "Aguardando processamento")
    worker = threading.Thread(
        target=_run_file_job,
        args=(job_id, content, file.filename or "", start_date, end_date, publisher, script_id, suspicious_traffic),
        daemon=True,
    )
    worker.start()
    return AnalyzeJobResponse(job_id=job_id)


@app.get("/status/{job_id}")
async def get_job_status(job_id: str):
    with _jobs_lock:
        status = _analysis_jobs.get(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return status


@app.get("/history")
async def get_history(limit: int = 10):
    safe_limit = max(1, min(limit, 100))
    with _history_lock:
        items = list(reversed(_analysis_history))[:safe_limit]
    compact = [
        {
            "id": item.get("id"),
            "timestamp": item.get("timestamp"),
            "publisher": item.get("publisher"),
            "script_id": item.get("script_id"),
            "start_date": item.get("start_date"),
            "end_date": item.get("end_date"),
            "status": item.get("status"),
        }
        for item in items
    ]
    return {"items": compact, "total": len(_analysis_history)}


@app.get("/history/{entry_id}")
async def get_history_by_id(entry_id: str):
    entry = _get_history_entry_by_id(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Análise não encontrada no histórico.")
    return entry


@app.get("/export/{fmt}")
async def export_analysis(fmt: str, analysis_id: str | None = None):
    target_id = analysis_id or _latest_analysis_id
    if not target_id:
        raise HTTPException(status_code=404, detail="Nenhuma análise disponível para exportação.")

    entry = _get_history_entry_by_id(target_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Análise solicitada não encontrada para exportação.")

    fmt_norm = (fmt or "").strip().lower()
    if fmt_norm == "json":
        return JSONResponse(content=entry)

    if fmt_norm == "csv":
        row = _flatten_for_csv(entry)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)
        content = output.getvalue().encode("utf-8")
        filename = f"analise_{target_id}.csv"
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    raise HTTPException(status_code=400, detail="Formato inválido. Use json ou csv.")
