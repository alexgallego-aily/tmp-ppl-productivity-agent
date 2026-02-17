"""
plots — Visualizations.

1. plot_manager_team_dashboard  — Per-team PPL KPI dashboard for a single manager
2. plot_domain_kpi_dashboard    — MNS domain KPI dashboard (BU + Country Org)
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .config import TEAM_KPI_PANELS, TEAM_PALETTE


# ===================================================================
# Helpers
# ===================================================================

def _hex_to_rgba(hex_color: str, alpha: float = 0.2) -> str:
    """Convert '#rrggbb' to 'rgba(r,g,b,alpha)'."""
    r = int(hex_color[1:3], 16)
    g = int(hex_color[3:5], 16)
    b = int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{alpha})"


# ===================================================================
# 1. MANAGER TEAM DASHBOARD (PPL KPIs)
# ===================================================================

def plot_manager_team_dashboard(
    data: pd.DataFrame,
    manager_code: str,
) -> go.Figure | None:
    """Per-team KPI dashboard for a specific manager.

    Shows 18 KPI panels (9 rows x 2 cols).  Each panel has one line
    per team (organization_level_code x geo_code).  Single-team managers
    get filled area charts for visual clarity.

    Parameters
    ----------
    data : DataFrame
        Output of ``load_manager_team_kpis(manager_code)``.
    manager_code : str
        Used only for the title label.

    Returns
    -------
    go.Figure or None if data is empty.
    """
    if len(data) == 0:
        print(f"No data for manager {manager_code}")
        return None

    # ── identify teams (ordered by avg headcount desc) ────────────
    teams = (
        data.groupby(["organization_level_code", "geo_code"])
        .agg(avg_hc=("headcount", "mean"))
        .sort_values("avg_hc", ascending=False)
        .reset_index()
    )
    n_teams = len(teams)
    single_team = n_teams == 1

    # ── labels & colours ──────────────────────────────────────────
    team_keys: list[tuple[str, str]] = []
    team_labels: dict[tuple[str, str], str] = {}
    team_colors: dict[tuple[str, str], str] = {}

    for i, row in teams.iterrows():
        key = (row["organization_level_code"], row["geo_code"])
        team_keys.append(key)
        if single_team:
            team_labels[key] = row["geo_code"]
        else:
            team_labels[key] = f"{row['organization_level_code'][:25]} · {row['geo_code']}"
        team_colors[key] = TEAM_PALETTE[i % len(TEAM_PALETTE)]

    # ── subplots ──────────────────────────────────────────────────
    panels = TEAM_KPI_PANELS
    n_panels = len(panels)
    n_rows = (n_panels + 1) // 2

    titles = [p[1] for p in panels]

    fig = make_subplots(
        rows=n_rows,
        cols=2,
        subplot_titles=titles,
        vertical_spacing=0.045,
        horizontal_spacing=0.10,
    )

    # ── populate panels ───────────────────────────────────────────
    for idx, (metric, _title, y_range, fill_single) in enumerate(panels):
        row = idx // 2 + 1
        col = idx % 2 + 1

        for key in team_keys:
            org, geo = key
            color = team_colors[key]
            team_data = data[
                (data["organization_level_code"] == org)
                & (data["geo_code"] == geo)
            ].sort_values("month")

            if metric not in team_data.columns or team_data[metric].isna().all():
                continue

            trace_kw: dict = {
                "x": team_data["month"],
                "y": team_data[metric],
                "name": team_labels[key],
                "line": dict(color=color, width=2.5 if single_team else 2),
                "mode": "lines+markers",
                "legendgroup": f"{org}_{geo}",
                "showlegend": (idx == 0),
            }

            if single_team and fill_single:
                trace_kw["fill"] = "tozeroy"
                trace_kw["fillcolor"] = _hex_to_rgba(color, 0.18)

            fig.add_trace(go.Scatter(**trace_kw), row=row, col=col)

        if y_range:
            fig.update_yaxes(range=y_range, row=row, col=col)

    # ── reference lines ───────────────────────────────────────────
    _ref_lines = {
        "team_health_score":       [("green", 80, "dot"), ("orange", 60, "dot")],
        "development_score":       [("green", 80, "dot"), ("orange", 60, "dot")],
        "mobility_score":          [("green", 80, "dot"), ("orange", 60, "dot")],
        "succession_score":        [("green", 80, "dot"), ("orange", 60, "dot")],
        "pct_female":              [("gray", 50, "dot")],
        "pct_critical_flight_risk": [("red", 10, "dot")],
    }
    for metric_name, lines in _ref_lines.items():
        panel_idx = next((i for i, p in enumerate(panels) if p[0] == metric_name), None)
        if panel_idx is None:
            continue
        r, c = panel_idx // 2 + 1, panel_idx % 2 + 1
        for color, y_val, dash in lines:
            fig.add_hline(
                y=y_val, line_dash=dash, line_color=color,
                opacity=0.35, row=r, col=c,
            )

    # ── layout ────────────────────────────────────────────────────
    geos = ", ".join(sorted(data["geo_code"].dropna().unique()))
    funcs = ", ".join(sorted(data["primary_function"].dropna().unique())[:3])
    latest = data[data["month"] == data["month"].max()]
    total_hc = latest["headcount"].sum()
    date_range = (
        f"{data['month'].min().strftime('%Y-%m')} → "
        f"{data['month'].max().strftime('%Y-%m')}"
    )

    fig.update_layout(
        height=300 * n_rows,
        title_text=(
            f"Manager: {manager_code[:40]}…<br>"
            f"<sub>{n_teams} team{'s' if n_teams > 1 else ''} · "
            f"{geos} · {funcs} · "
            f"HC {total_hc:.0f} · {date_range}</sub>"
        ),
        showlegend=True,
        legend=dict(
            x=1.02, y=0.98, xanchor="left", yanchor="top",
            bgcolor="rgba(255,255,255,0.9)", font=dict(size=9),
        ),
        hovermode="x unified",
        margin=dict(r=260, t=110, b=60, l=60),
    )

    return fig


# ===================================================================
# 2. DOMAIN KPI DASHBOARD (MNS)
# ===================================================================

_DOMAIN_CLUSTER_PALETTE = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728',
    '#9467bd', '#8c564b', '#e377c2', '#7f7f7f',
    '#bcbd22', '#17becf', '#aec7e8', '#ffbb78',
    '#98df8a', '#ff9896', '#c5b0d5', '#c49c94',
]

MAX_DOMAIN_PANELS = 12


def plot_domain_kpi_dashboard(
    domain_df: pd.DataFrame,
    manager_code: str,
    business_unit: str,
    kpi_mapping_label: str = "",
) -> go.Figure | None:
    """Dashboard for MNS domain KPIs — one panel per KPI code.

    Splits into BU KPIs and Country Organisation KPIs.
    Each panel shows one line per cluster, with optional dashed target line.

    Parameters
    ----------
    domain_df : DataFrame
        Output of ``load_manager_domain_kpis()``.
        Expected columns: kpi_code, business_unit_label, cluster_label,
        kpi_facts_date, kpi_value, target_value, source.
    manager_code : str
        For the title label.
    business_unit : str
        E.g. ``'General Medicine'``.
    kpi_mapping_label : str
        Human-readable label (e.g. ``'General Medicine'``).

    Returns
    -------
    go.Figure or None if data is empty.
    """
    if len(domain_df) == 0:
        return None

    # ── Rank KPIs by data richness (clusters x date points) ────────
    kpi_rank = (
        domain_df.groupby(["source", "kpi_code"])
        .agg(
            n_clusters=("cluster_label", "nunique"),
            n_dates=("kpi_facts_date", "nunique"),
            n_rows=("kpi_value", "count"),
        )
        .assign(score=lambda d: d["n_clusters"] * d["n_dates"])
        .sort_values("score", ascending=False)
        .reset_index()
    )

    # Collect BU KPIs per-cluster, BU aggregate, then Country Org
    bu_kpis = kpi_rank[kpi_rank["source"] == "domain"]["kpi_code"].tolist()
    ba_kpis = kpi_rank[kpi_rank["source"] == "bu_aggregate"]["kpi_code"].tolist()
    co_kpis = kpi_rank[kpi_rank["source"] == "country_org"]["kpi_code"].tolist()

    # Limit total panels — balance across sources
    n_sources = sum(1 for lst in [bu_kpis, ba_kpis, co_kpis] if lst)
    per_source = MAX_DOMAIN_PANELS // max(n_sources, 1)
    selected_bu = bu_kpis[:per_source] if n_sources > 1 else bu_kpis[:MAX_DOMAIN_PANELS]
    remaining = MAX_DOMAIN_PANELS - len(selected_bu)
    selected_ba = ba_kpis[:remaining // 2] if co_kpis else ba_kpis[:remaining]
    remaining -= len(selected_ba)
    selected_co = co_kpis[:remaining]

    all_kpis = (
        [(kpi, "domain") for kpi in selected_bu]
        + [(kpi, "bu_aggregate") for kpi in selected_ba]
        + [(kpi, "country_org") for kpi in selected_co]
    )

    if not all_kpis:
        return None

    n_panels = len(all_kpis)
    n_cols = 2
    n_rows = (n_panels + n_cols - 1) // n_cols

    _source_prefix = {"domain": "BU", "bu_aggregate": "BU Agg", "country_org": "Country"}
    titles = []
    for kpi_code, source in all_kpis:
        prefix = _source_prefix.get(source, source)
        titles.append(f"{prefix}: {kpi_code}")

    # pad to even
    if n_panels % 2 == 1:
        titles.append("")

    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=titles,
        vertical_spacing=max(0.03, 0.30 / n_rows),
        horizontal_spacing=0.10,
    )

    # ── Colour map per cluster ─────────────────────────────────────
    all_clusters = sorted(domain_df["cluster_label"].dropna().unique())
    # Display-friendly name for the sentinel aggregate label
    _display_name = lambda cl: "BU Total" if cl == "__BU_AGGREGATE__" else cl
    cluster_colors = {
        cl: _DOMAIN_CLUSTER_PALETTE[i % len(_DOMAIN_CLUSTER_PALETTE)]
        for i, cl in enumerate(all_clusters)
    }

    shown_legends = set()

    for panel_idx, (kpi_code, source) in enumerate(all_kpis):
        row = panel_idx // n_cols + 1
        col = panel_idx % n_cols + 1

        subset = domain_df[
            (domain_df["kpi_code"] == kpi_code) & (domain_df["source"] == source)
        ].sort_values("kpi_facts_date")

        if len(subset) == 0:
            continue

        for cl in sorted(subset["cluster_label"].dropna().unique()):
            cl_data = subset[subset["cluster_label"] == cl].sort_values("kpi_facts_date")
            color = cluster_colors.get(cl, "#999999")
            cl_label = _display_name(cl)
            show_legend = cl_label not in shown_legends
            shown_legends.add(cl_label)

            # BU aggregate gets a thicker line
            line_width = 3 if cl == "__BU_AGGREGATE__" else 2

            fig.add_trace(
                go.Scatter(
                    x=cl_data["kpi_facts_date"],
                    y=cl_data["kpi_value"],
                    name=cl_label,
                    mode="lines+markers",
                    line=dict(color=color, width=line_width),
                    legendgroup=cl_label,
                    showlegend=show_legend,
                ),
                row=row, col=col,
            )

            # target line (dashed)
            if cl_data["target_value"].notna().any():
                fig.add_trace(
                    go.Scatter(
                        x=cl_data["kpi_facts_date"],
                        y=cl_data["target_value"],
                        name=f"{cl_label} target",
                        mode="lines",
                        line=dict(color=color, width=1.2, dash="dot"),
                        legendgroup=cl_label,
                        showlegend=False,
                    ),
                    row=row, col=col,
                )

    # ── layout ─────────────────────────────────────────────────────
    n_bu = len(selected_bu)
    n_ba = len(selected_ba)
    n_co = len(selected_co)
    date_range = (
        f"{domain_df['kpi_facts_date'].min().strftime('%Y-%m')} → "
        f"{domain_df['kpi_facts_date'].max().strftime('%Y-%m')}"
    )

    fig.update_layout(
        height=300 * n_rows,
        title_text=(
            f"Domain KPIs: {kpi_mapping_label or business_unit}<br>"
            f"<sub>Manager: {manager_code[:40]}… | "
            f"{n_bu} BU · {n_ba} BU Agg · {n_co} Country KPIs · "
            f"{len(all_clusters)} clusters · {date_range}</sub>"
        ),
        showlegend=True,
        legend=dict(
            x=1.02, y=0.98, xanchor="left", yanchor="top",
            bgcolor="rgba(255,255,255,0.9)", font=dict(size=9),
        ),
        hovermode="x unified",
        margin=dict(r=260, t=110, b=60, l=60),
    )

    return fig
