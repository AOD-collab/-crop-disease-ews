"""Shared dashboard colours (one colour per disease everywhere) and small widgets."""

# categorical hues in fixed order (maize-field greens, yellows, browns)
CATEGORICAL_HUES = ["#2E7D32", "#E0A800", "#8D5A2B", "#1F7A8C", "#9BBF3B", "#C8541B", "#5B3A29", "#6A994E"]
# one fixed colour per disease, the same in every chart
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
    """{class: colour}: the disease colour, else the next categorical hue."""
    out, i = {}, 0
    for cls in classes:
        if str(cls).lower() in DISEASE_COLOR_MAP:
            out[cls] = DISEASE_COLOR_MAP[str(cls).lower()]
        else:
            out[cls] = CATEGORICAL_HUES[i % len(CATEGORICAL_HUES)]
            i += 1
    return out


def high_risk_table(df, summary: list, summary_label: str) -> None:
    """The per-municipality HIGH-risk table with `summary` as its middle column."""
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
    """One row of st.metric cards; each spec is (label, value[, help])."""
    import streamlit as st
    for col, (label, value, *help_text) in zip(st.columns(len(specs)), specs):
        col.metric(label, value, help=help_text[0] if help_text else None)
