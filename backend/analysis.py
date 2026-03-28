"""
analysis.py — Computes discrepancy metrics between publisher and Clever data.
"""

from dataclasses import dataclass


@dataclass
class DiscrepancyResult:
    # Publisher figures
    pub_served: int
    pub_viewable: int
    pub_viewability: float        # 0.0–1.0

    # Clever / internal figures
    clever_served: int
    clever_viewable: int
    clever_viewability: float     # 0.0–1.0

    # Differences
    diff_served: int              # absolute
    diff_viewable: int            # absolute
    viewability_diff_pp: float    # percentage points (e.g. 5.3 means 5.3 pp)
    viewability_diff_pct: float   # relative % difference

    # Classification
    status: str                   # "Normal" | "Attention" | "High Discrepancy"


def _safe_rate(viewable: int, served: int) -> float:
    """Returns viewable/served as a float, or 0.0 if served is 0."""
    if served == 0:
        return 0.0
    return viewable / served


def _classify(viewability_diff_pp: float) -> str:
    """Classify discrepancy based on absolute viewability difference in percentage points."""
    abs_diff = abs(viewability_diff_pp)
    if abs_diff < 15:
        return "Normal"
    elif abs_diff <= 25:
        return "Attention"
    else:
        return "High Discrepancy"


def calculate_discrepancy(
    pub_served: int,
    pub_viewable: int,
    clever_served: int,
    clever_viewable: int,
) -> DiscrepancyResult:
    """
    Computes all discrepancy metrics between publisher and Clever data.
    """
    pub_viewability    = _safe_rate(pub_viewable, pub_served)
    clever_viewability = _safe_rate(clever_viewable, clever_served)

    diff_served   = abs(pub_served   - clever_served)
    diff_viewable = abs(pub_viewable - clever_viewable)

    # Viewability difference in percentage points (pub − clever)
    viewability_diff_pp = (pub_viewability - clever_viewability) * 100

    # Relative percentage difference vs Clever baseline
    if clever_viewability > 0:
        viewability_diff_pct = ((pub_viewability - clever_viewability) / clever_viewability) * 100
    else:
        viewability_diff_pct = 0.0

    status = _classify(viewability_diff_pp)

    return DiscrepancyResult(
        pub_served=pub_served,
        pub_viewable=pub_viewable,
        pub_viewability=pub_viewability,
        clever_served=clever_served,
        clever_viewable=clever_viewable,
        clever_viewability=clever_viewability,
        diff_served=diff_served,
        diff_viewable=diff_viewable,
        viewability_diff_pp=viewability_diff_pp,
        viewability_diff_pct=viewability_diff_pct,
        status=status,
    )
