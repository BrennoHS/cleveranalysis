"""
parser.py — Uses Gemini API to extract structured metrics from raw publisher reports.
"""

import os
import json
import re
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=True)

genai.configure(api_key=os.environ["GEMINI_API_KEY"])

PARSER_MODEL = os.environ.get("GEMINI_PARSER_MODEL", "gemini-flash-lite-latest")
PARSER_MAX_OUTPUT_TOKENS = int(os.environ.get("GEMINI_PARSER_MAX_OUTPUT_TOKENS", "256"))


EXTRACTION_PROMPT = """
You are an AdOps data extraction assistant. Your ONLY task is to extract metrics from raw text and return a JSON object.

Extract ONLY these 4 metrics from the publisher report:

1. served_impressions — An integer (no commas, decimals or text)
2. viewable_impressions — An integer (no commas, decimals or text)
3. start_date — ISO format YYYY-MM-DD string, or null
4. end_date — ISO format YYYY-MM-DD string, or null

Rules:
- Return ONLY a valid JSON object with no other text
- Do not include markdown fences (no ``` symbols)
- Do not add any explanation or comments
- If a value is missing, use null
- Numbers must be plain integers only

Publisher Report:
\"\"\"
{report_text}
\"\"\"

Return ONLY this JSON (nothing else):
{{"served_impressions": <number_or_null>, "viewable_impressions": <number_or_null>, "start_date": "<YYYY-MM-DD_or_null>", "end_date": "<YYYY-MM-DD_or_null>"}}
"""


def _fallback_extract_from_text(report_text: str) -> dict:
    """Best-effort extraction if model output is malformed."""
    text = report_text or ""

    # Extract first two large numeric values as served/viewable fallback.
    nums = re.findall(r"\b\d{3,}\b", text)
    served = int(nums[0]) if len(nums) >= 1 else None
    viewable = int(nums[1]) if len(nums) >= 2 else None

    # Extract dates in YYYY-MM-DD format.
    dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", text)
    start_date = dates[0] if len(dates) >= 1 else None
    end_date = dates[1] if len(dates) >= 2 else None

    return {
        "served_impressions": served,
        "viewable_impressions": viewable,
        "start_date": start_date,
        "end_date": end_date,
    }


def parse_publisher_report(report_text: str) -> dict:
    """
    Sends the raw publisher report to Gemini and returns extracted metrics.
    Returns a dict with keys: served_impressions, viewable_impressions, start_date, end_date.
    """
    model = genai.GenerativeModel(PARSER_MODEL)

    prompt = EXTRACTION_PROMPT.format(report_text=report_text.strip())

    response = model.generate_content(
        prompt,
        generation_config=genai.types.GenerationConfig(
            temperature=0.0,
            max_output_tokens=PARSER_MAX_OUTPUT_TOKENS,
        ),
    )

    raw = response.text.strip()

    # Strip markdown fences if Gemini adds them despite instructions
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    # Try to extract JSON object from the response
    # In case there's extra text before/after
    json_match = re.search(r'\{.*\}', raw, re.DOTALL)
    if json_match:
        raw = json_match.group(0)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        fallback = _fallback_extract_from_text(report_text)
        if fallback.get("served_impressions") and fallback.get("start_date") and fallback.get("end_date"):
            return fallback
        # More helpful error message
        raise ValueError(f"Failed to parse JSON from Gemini response: {str(e)}\nRaw response: {raw[:200]}")

    # Sanitize: ensure numeric fields are ints if present
    for field in ("served_impressions", "viewable_impressions"):
        val = parsed.get(field)
        if val is not None:
            parsed[field] = int(str(val).replace(",", "").replace(".", "").strip())

    return parsed
