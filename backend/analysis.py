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
    clever_garbage: int
    clever_adjusted_vi: int
    clever_viewability: float     # 0.0–1.0

    # Differences
    diff_served: int              # absolute
    diff_viewable: int            # absolute
    served_discrepancy_pct: float
    viewable_discrepancy_pct: float
    viewability_diff_pp: float    # percentage points (e.g. 5.3 means 5.3 pp)
    viewability_diff_pct: float   # relative % difference

    # Classification
    status: str                   # "Baixa discrepância" | "Atenção na discrepância" | "Alta discrepância"


def _safe_rate(viewable: int, served: int) -> float:
    """Returns viewable/served as a float, or 0.0 if served is 0."""
    if served == 0:
        return 0.0
    return viewable / served


def _classify(discrepancy_pct: float) -> str:
    """Classify discrepancy by percentage bands."""
    if discrepancy_pct < 15:
        return "Baixa discrepância"
    if discrepancy_pct <= 30:
        return "Atenção na discrepância"
    return "Alta discrepância"


def _discrepancy_pct(a: float, b: float) -> float:
    """Discrepancy (%) = 100 - ((smaller * 100) / larger)."""
    larger = max(a, b)
    smaller = min(a, b)
    if larger <= 0:
        return 0.0
    return 100 - ((smaller * 100) / larger)


def calculate_discrepancy(
    pub_served: int,
    pub_viewable: int,
    clever_served: int,
    clever_viewable: int,
    clever_garbage: int,
) -> DiscrepancyResult:
    """
    Computes all discrepancy metrics between publisher and Clever data.
    """
    clever_adjusted_vi = clever_viewable + clever_garbage
    pub_viewability    = _safe_rate(pub_viewable, pub_served)
    clever_viewability = _safe_rate(clever_adjusted_vi, clever_served)

    diff_served   = abs(pub_served   - clever_served)
    diff_viewable = abs(pub_viewable - clever_adjusted_vi)
    served_discrepancy_pct = _discrepancy_pct(pub_served, clever_served)
    viewable_discrepancy_pct = _discrepancy_pct(pub_viewable, clever_adjusted_vi)

    # Viewability difference in percentage points (pub − clever)
    viewability_diff_pp = (pub_viewability - clever_viewability) * 100

    viewability_diff_pct = _discrepancy_pct(pub_viewability * 100, clever_viewability * 100)

    status = _classify(viewable_discrepancy_pct)

    return DiscrepancyResult(
        pub_served=pub_served,
        pub_viewable=pub_viewable,
        pub_viewability=pub_viewability,
        clever_served=clever_served,
        clever_viewable=clever_adjusted_vi,
        clever_garbage=clever_garbage,
        clever_adjusted_vi=clever_adjusted_vi,
        clever_viewability=clever_viewability,
        diff_served=diff_served,
        diff_viewable=diff_viewable,
        served_discrepancy_pct=served_discrepancy_pct,
        viewable_discrepancy_pct=viewable_discrepancy_pct,
        viewability_diff_pp=viewability_diff_pp,
        viewability_diff_pct=viewability_diff_pct,
        status=status,
    )
