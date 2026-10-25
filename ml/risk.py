"""
Risk level from the CNN's classification (DIT manual section 26):
rule-based thresholds (config/settings.RISK_THRESHOLDS) on the model's
probability that the leaf is diseased.
"""

from config.settings import RISK_THRESHOLDS


def compute_risk(features: dict, is_uncertain: bool = False) -> tuple:
    """Rule-based, explainable risk indicator: the model's probability that
    the leaf is diseased at all (lesion_score = 1 - P(healthy)), put into
    LOW / MEDIUM / HIGH by RISK_THRESHOLDS. Deliberately NOT the CNN's
    top-class confidence (section 26): a confident "healthy" is low risk.

    `is_uncertain` (set when the CNN's top-class confidence fell below
    IMAGE_MODEL["confidence_threshold"] — see ml/inference.py) caps the
    result below HIGH: an unreliable read must not raise a HIGH-risk alert.
    """
    score = round(min(max(features.get("lesion_score") or 0.0, 0.0), 1.0), 3)

    level = next((lvl for lvl in ("HIGH", "MEDIUM") if score >= RISK_THRESHOLDS[lvl.lower()]), "LOW")
    if is_uncertain and level == "HIGH":
        level = "MEDIUM"

    return score, level
