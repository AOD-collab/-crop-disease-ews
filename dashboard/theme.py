"""Chart colours shared by every dashboard page (dashboard/app.py and the
pages it imports), so a disease has the same colour everywhere."""

# Fixed-order categorical hues for open-ended series (disease classes,
# locations) — assigned in order, never cycled past what a chart actually
# needs.
# Maize-field hues (greens, kernel yellows, earth browns, one teal/blue for
# separation) — distinct from each other, in harmony with the green/yellow UI.
CATEGORICAL_HUES = ["#2E7D32", "#E0A800", "#8D5A2B", "#1F7A8C", "#9BBF3B", "#C8541B", "#5B3A29", "#6A994E"]
# Fixed colour per disease, the same in every chart: healthy = leaf green,
# each disease an earthy tone that recalls how it looks on the leaf.
DISEASE_COLOR_MAP = {
    "healthy": "#2E7D32",
    "common_rust": "#C8541B",            # rust orange
    "gray_leaf_spot": "#7D7F6A",         # grey-olive lesions
    "northern_leaf_blight": "#8D5A2B",   # tan-brown blight
    "uncertain": "#E0A800",
}
STATUS_BADGE_COLOR = {
    "COMPLETED": "green", "INGESTED": "blue", "PROCESSING": "blue",
    "PENDING_REVIEW": "gray", "UPLOADED": "gray", "FAILED": "red",
}


def disease_colors(classes) -> dict:
    """{class: colour} for the classes a chart actually shows: the fixed
    disease colour when there is one, otherwise the next categorical hue."""
    out, i = {}, 0
    for cls in classes:
        if str(cls).lower() in DISEASE_COLOR_MAP:
            out[cls] = DISEASE_COLOR_MAP[str(cls).lower()]
        else:
            out[cls] = CATEGORICAL_HUES[i % len(CATEGORICAL_HUES)]
            i += 1
    return out
