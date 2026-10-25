"""
Risk level from the CNN's classification (DIT manual section 26):
rule-based thresholds (config/settings.RISK_THRESHOLDS) on the model's
probability that the leaf is diseased.
"""

from config.settings import RISK_THRESHOLDS


def compute_risk(features: dict, is_uncertain: bool = False) -> tuple:
    """(risk_score, LOW/MEDIUM/HIGH) from P(diseased). An uncertain
    prediction is never HIGH."""
    score = round(min(max(features.get("lesion_score") or 0.0, 0.0), 1.0), 3)

    level = next((lvl for lvl in ("HIGH", "MEDIUM") if score >= RISK_THRESHOLDS[lvl.lower()]), "LOW")
    if is_uncertain and level == "HIGH":
        level = "MEDIUM"

    return score, level
