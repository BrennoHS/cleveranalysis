"""
ai_explainer.py — Generates a professional discrepancy explanation using Gemini.
"""

import os
import google.generativeai as genai
from dotenv import load_dotenv
from analysis import DiscrepancyResult

load_dotenv()

genai.configure(api_key=os.environ["GEMINI_API_KEY"])


EXPLANATION_PROMPT = """
You are a senior AdOps analyst writing a professional discrepancy report for an internal team.

Here is the comparison data between the publisher's report and our internal Clever ad server:

Publisher Data:
- Served Impressions: {pub_served:,}
- Viewable Impressions: {pub_viewable:,}
- Viewability Rate: {pub_viewability:.1f}%

Clever (Internal) Data:
- Served Impressions: {clever_served:,}
- Viewable Impressions: {clever_viewable:,}
- Viewability Rate: {clever_viewability:.1f}%

Discrepancy Metrics:
- Served Impressions Difference: {diff_served:,}
- Viewable Impressions Difference: {diff_viewable:,}
- Viewability Difference: {viewability_diff_pp:+.1f} percentage points
- Status: {status}

Period: {start_date} to {end_date}
Publisher / Domain: {publisher}

Write a professional 2–3 paragraph analysis that:
1. Clearly states what the data shows and whether the discrepancy is within acceptable range
2. Compares both sides objectively
3. Suggests 2–3 realistic possible causes (e.g., measurement methodology differences, tracking tag placement, ad rendering timing, IVT filtering differences, viewability vendor discrepancies)

Keep the tone professional and factual. Do NOT invent specific technical details not supported by the data. Do NOT use bullet points — write in flowing paragraphs.
"""


def generate_explanation(
    result: DiscrepancyResult,
    start_date: str,
    end_date: str,
    publisher: str,
) -> str:
    """
    Calls Gemini to generate a professional explanation of the discrepancy.
    Returns a plain text explanation string.
    """
    model = genai.GenerativeModel("gemini-2.5-pro")

    prompt = EXPLANATION_PROMPT.format(
        pub_served=result.pub_served,
        pub_viewable=result.pub_viewable,
        pub_viewability=result.pub_viewability * 100,
        clever_served=result.clever_served,
        clever_viewable=result.clever_viewable,
        clever_viewability=result.clever_viewability * 100,
        diff_served=result.diff_served,
        diff_viewable=result.diff_viewable,
        viewability_diff_pp=result.viewability_diff_pp,
        status=result.status,
        start_date=start_date,
        end_date=end_date,
        publisher=publisher or "Not specified",
    )

    response = model.generate_content(
        prompt,
        generation_config=genai.types.GenerationConfig(
            temperature=0.3,
            max_output_tokens=600,
        ),
    )

    return response.text.strip()
