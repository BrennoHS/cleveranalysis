"""
parser.py — Uses Gemini API to extract structured metrics from raw publisher reports.
"""

import os
import json
import re
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

genai.configure(api_key=os.environ["GEMINI_API_KEY"])


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


def parse_publisher_report(report_text: str) -> dict:
    """
    Sends the raw publisher report to Gemini and returns extracted metrics.
    Returns a dict with keys: served_impressions, viewable_impressions, start_date, end_date.
    """
    model = genai.GenerativeModel("gemini-2.5-flash")

    prompt = EXTRACTION_PROMPT.format(report_text=report_text.strip())

    response = model.generate_content(
        prompt,
        generation_config=genai.types.GenerationConfig(
            temperature=0.0,
            max_output_tokens=512,
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
        # More helpful error message
        raise ValueError(f"Failed to parse JSON from Gemini response: {str(e)}\nRaw response: {raw[:200]}")

    # Sanitize: ensure numeric fields are ints if present
    for field in ("served_impressions", "viewable_impressions"):
        val = parsed.get(field)
        if val is not None:
            parsed[field] = int(str(val).replace(",", "").replace(".", "").strip())

    return parsed
