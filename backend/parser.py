"""
parser.py - Deterministic extraction of publisher metrics from raw reports.
Optional Gemini fallback can be enabled via GEMINI_PARSER_ENABLED.
"""

import json
import os
import re
from datetime import date
from typing import Optional

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=True)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


PARSER_ENABLED = _env_bool("GEMINI_PARSER_ENABLED", False)
PARSER_MODEL = os.environ.get("GEMINI_PARSER_MODEL", "gemini-flash-lite-latest")
PARSER_MAX_OUTPUT_TOKENS = int(os.environ.get("GEMINI_PARSER_MAX_OUTPUT_TOKENS", "256"))

SERVED_LABELS = [
    "served impressions",
    "impressions served",
    "impressions",
    "measurable",
    "total impressions",
    "impressões servidas",
    "impressoes servidas",
    "impressões",
    "impressoes",
    "total de impressões",
    "total de impressoes",
]

VIEWABLE_LABELS = [
    "viewable impressions",
    "viewable",
    "active views",
    "active view",
    "viewable ads",
    "visualizáveis",
    "visualizaveis",
    "impressões visualizáveis",
    "impressoes visualizaveis",
    "visualizações",
    "visualizacoes",
    "taxa de visualização",
    "taxa de visualizacao",
]

SERVED_ALIASES = ["si", "served", "served impression", "served impressions"]
VIEWABLE_ALIASES = ["vi", "viewable", "viewable impression", "viewable impressions", "active view", "active views"]

MONTHS = {
    "jan": 1,
    "january": 1,
    "fev": 2,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "abr": 4,
    "apr": 4,
    "april": 4,
    "mai": 5,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "ago": 8,
    "aug": 8,
    "august": 8,
    "set": 9,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "out": 10,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dez": 12,
    "dec": 12,
    "december": 12,
}

MONTH_NAMES_PATTERN = (
    r"jan(?:uary)?|fev|feb(?:ruary)?|mar(?:ch)?|abr|apr(?:il)?|mai|may|"
    r"jun(?:e)?|jul(?:y)?|ago|aug(?:ust)?|set|sep(?:t(?:ember)?)?|"
    r"out|oct(?:ober)?|nov(?:ember)?|dez|dec(?:ember)?"
)

DATE_TOKEN = (
    r"(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|"
    r"\d{1,2}\s+(?:" + MONTH_NAMES_PATTERN + r")\s+\d{4}|"
    r"(?:" + MONTH_NAMES_PATTERN + r")\s+\d{1,2},?\s+\d{4})"
)

EXTRACTION_PROMPT = """
You are an AdOps data extraction assistant. Your ONLY task is to extract metrics from raw text and return a JSON object.

Extract ONLY these 4 metrics from the publisher report:
1. served_impressions (integer)
2. viewable_impressions (integer)
3. start_date (ISO YYYY-MM-DD or null)
4. end_date (ISO YYYY-MM-DD or null)

Rules:
- Return ONLY valid JSON, no markdown
- If missing, use null

Publisher Report:
\"\"\"
{report_text}
\"\"\"

Return only:
{{"served_impressions": <number_or_null>, "viewable_impressions": <number_or_null>, "start_date": "<YYYY-MM-DD_or_null>", "end_date": "<YYYY-MM-DD_or_null>"}}
"""


def _normalize_number(raw: str) -> Optional[int]:
    if raw is None:
        return None
    digits = re.sub(r"[^\d]", "", str(raw))
    if not digits:
        return None
    return int(digits)


def _extract_labeled_metric(text: str, labels: list[str]) -> Optional[int]:
    for label in labels:
        escaped = re.escape(label)

        # Label-first lines, including table-like separators.
        pattern_primary = re.compile(
            rf"(?im)^\s*(?:{escaped})\b[^\d\n]{{0,24}}([\d][\d,\.\s]{{2,}})",
        )
        match = pattern_primary.search(text)
        if match:
            value = _normalize_number(match.group(1))
            if value is not None:
                return value

        # More permissive inline fallback when label appears in sentence text.
        pattern_fallback = re.compile(
            rf"(?i)\b(?:{escaped})\b[^\d\n]{{0,40}}([\d][\d,\.\s]{{2,}})",
        )
        match = pattern_fallback.search(text)
        if match:
            value = _normalize_number(match.group(1))
            if value is not None:
                return value

    return None


def _extract_alias_metric_from_lines(text: str, aliases: list[str]) -> Optional[int]:
    """Extract metric from compact lines such as '9000 vi' or 'si: 10000'."""
    lines = (text or "").splitlines()

    for line in lines:
        line_text = (line or "").strip()
        if not line_text:
            continue

        for alias in aliases:
            esc = re.escape(alias)

            # Number-first compact format: 9000 vi
            m_num_first = re.search(rf"(?i)\b([\d][\d,\.\s]{{0,}})\s*(?:{esc})\b", line_text)
            if m_num_first:
                value = _normalize_number(m_num_first.group(1))
                if value is not None:
                    return value

            # Label-first compact format: si 10000 / si: 10000
            m_label_first = re.search(rf"(?i)\b(?:{esc})\b\s*[:=\-]?\s*([\d][\d,\.\s]{{0,}})", line_text)
            if m_label_first:
                value = _normalize_number(m_label_first.group(1))
                if value is not None:
                    return value

    return None


def _to_iso(y: int, m: int, d: int) -> Optional[str]:
    try:
        return date(y, m, d).isoformat()
    except ValueError:
        return None


def _parse_numeric_date(raw: str, prefer_day_first: bool) -> Optional[str]:
    parts = re.split(r"[/-]", raw.strip())
    if len(parts) != 3:
        return None

    a, b, c = [p.strip() for p in parts]
    if len(a) == 4:
        return _to_iso(int(a), int(b), int(c))

    if len(c) == 2:
        year = 2000 + int(c)
    else:
        year = int(c)

    p1 = int(a)
    p2 = int(b)

    if p1 > 12:
        day, month = p1, p2
    elif p2 > 12:
        month, day = p1, p2
    else:
        if prefer_day_first:
            day, month = p1, p2
        else:
            month, day = p1, p2

    return _to_iso(year, month, day)


def _parse_textual_date(raw: str) -> Optional[str]:
    s = re.sub(r",", "", raw.lower()).strip()

    # Month-first: Jan 31 2025
    m1 = re.match(rf"^(?P<mon>{MONTH_NAMES_PATTERN})\s+(?P<day>\d{{1,2}})\s+(?P<year>\d{{4}})$", s)
    if m1:
        month = MONTHS.get(m1.group("mon"))
        return _to_iso(int(m1.group("year")), int(month), int(m1.group("day"))) if month else None

    # Day-first: 31 Jan 2025
    m2 = re.match(rf"^(?P<day>\d{{1,2}})\s+(?P<mon>{MONTH_NAMES_PATTERN})\s+(?P<year>\d{{4}})$", s)
    if m2:
        month = MONTHS.get(m2.group("mon"))
        return _to_iso(int(m2.group("year")), int(month), int(m2.group("day"))) if month else None

    return None


def _parse_date_token(raw: str, prefer_day_first: bool) -> Optional[str]:
    token = raw.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", token):
        return token
    if re.match(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$", token):
        return _parse_numeric_date(token, prefer_day_first)
    return _parse_textual_date(token)


def _extract_dates(text: str) -> tuple[Optional[str], Optional[str]]:
    prefer_day_first = bool(re.search(r"\b(periodo|publisher|relatorio)\b", text.lower()))

    range_pattern = re.compile(
        rf"(?i)(?:period|periodo|from|de)\s*[:\-]?\s*(?P<d1>{DATE_TOKEN})\s*(?:to|ate|a|\-|\u2013|\u2014)\s*(?P<d2>{DATE_TOKEN})",
    )
    m = range_pattern.search(text)
    if m:
        start = _parse_date_token(m.group("d1"), prefer_day_first)
        end = _parse_date_token(m.group("d2"), prefer_day_first)
        if start and end:
            return (start, end) if start <= end else (end, start)

    candidates: list[tuple[int, str]] = []
    token_pattern = re.compile(DATE_TOKEN, re.IGNORECASE)
    for match in token_pattern.finditer(text):
        parsed = _parse_date_token(match.group(0), prefer_day_first)
        if parsed:
            candidates.append((match.start(), parsed))

    if len(candidates) >= 2:
        candidates.sort(key=lambda x: x[0])
        start, end = candidates[0][1], candidates[1][1]
        return (start, end) if start <= end else (end, start)

    if len(candidates) == 1:
        return candidates[0][1], None

    return None, None


def _deterministic_extract(report_text: str) -> dict:
    text = report_text or ""

    # First, handle compact aliases on separate lines (e.g. "9000 vi", "10000 si").
    served = _extract_alias_metric_from_lines(text, SERVED_ALIASES)
    viewable = _extract_alias_metric_from_lines(text, VIEWABLE_ALIASES)

    # Then apply broader labeled parsing for standard report layouts.
    if served is None:
        served = _extract_labeled_metric(text, SERVED_LABELS)
    if viewable is None:
        viewable = _extract_labeled_metric(text, VIEWABLE_LABELS)

    if served is None or viewable is None:
        # Last-resort fallback: first numeric tokens >= 3 digits.
        nums = [_normalize_number(n) for n in re.findall(r"\b\d[\d,\.\s]{2,}\b", text)]
        nums = [n for n in nums if n is not None]
        if served is None and len(nums) >= 1:
            served = nums[0]
        if viewable is None and len(nums) >= 2:
            viewable = nums[1]

    start_date, end_date = _extract_dates(text)

    return {
        "served_impressions": served,
        "viewable_impressions": viewable,
        "start_date": start_date,
        "end_date": end_date,
    }


def _parse_ai_json(raw: str, report_text: str) -> dict:
    text = (raw or "").strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    json_match = re.search(r"\{.*\}", text, re.DOTALL)
    if json_match:
        text = json_match.group(0)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        fallback = _deterministic_extract(report_text)
        if fallback.get("served_impressions") and fallback.get("start_date") and fallback.get("end_date"):
            return fallback
        raise ValueError(f"Falha ao interpretar JSON retornado pelo Gemini: {exc}") from exc

    for field in ("served_impressions", "viewable_impressions"):
        parsed[field] = _normalize_number(parsed.get(field))

    return {
        "served_impressions": parsed.get("served_impressions"),
        "viewable_impressions": parsed.get("viewable_impressions"),
        "start_date": parsed.get("start_date"),
        "end_date": parsed.get("end_date"),
    }


def _parse_with_gemini(report_text: str) -> dict:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing, cannot use parser fallback model")

    import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(PARSER_MODEL)

    prompt = EXTRACTION_PROMPT.format(report_text=report_text.strip())
    response = model.generate_content(
        prompt,
        generation_config=genai.types.GenerationConfig(
            temperature=0.0,
            max_output_tokens=PARSER_MAX_OUTPUT_TOKENS,
        ),
    )
    return _parse_ai_json(response.text or "", report_text)


def parse_publisher_report(report_text: str) -> dict:
    """
    Deterministic parser by default.
    If GEMINI_PARSER_ENABLED=true, Gemini can complement missing fields.
    """
    deterministic = _deterministic_extract(report_text)
    required = ("served_impressions", "start_date", "end_date")

    if all(deterministic.get(field) is not None for field in required):
        return deterministic

    if not PARSER_ENABLED:
        return deterministic

    parsed_ai = _parse_with_gemini(report_text)

    # Keep deterministic values first, and fill only missing fields with AI output.
    merged = deterministic.copy()
    for key in ("served_impressions", "viewable_impressions", "start_date", "end_date"):
        if merged.get(key) is None and parsed_ai.get(key) is not None:
            merged[key] = parsed_ai[key]

    return merged
