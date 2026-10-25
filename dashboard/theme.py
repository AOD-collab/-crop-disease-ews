"""Chart colours shared by every dashboard page (dashboard/app.py and the
pages it imports), so a disease has the same colour everywhere — and the
HIGH-risk-by-municipality table two pages show."""

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
    "COMPLETED": "green", "INGESTED": "blue", "PROCESSING": "blue", "FAILED": "red",
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


def high_risk_table(df, summary: list, summary_label: str) -> None:
    """The per-municipality HIGH-risk table (location, high, total, high_pct)
    with `summary` ("n of N ...") as its middle column."""
    import streamlit as st
    st.dataframe(
        df.assign(summary=summary),
        column_order=["location", "summary", "high_pct"],
        column_config={
            "location": st.column_config.TextColumn("Municipality"),
            "summary": st.column_config.TextColumn(summary_label),
            "high_pct": st.column_config.ProgressColumn("Share HIGH", format="%.2f%%", min_value=0, max_value=100),
        },
        hide_index=True, width="stretch",
    )


def metric_row(*specs) -> None:
    """One row of st.metric cards, side by side: each spec is
    (label, value) or (label, value, help)."""
    import streamlit as st
    for col, (label, value, *help_text) in zip(st.columns(len(specs)), specs):
        col.metric(label, value, help=help_text[0] if help_text else None)
