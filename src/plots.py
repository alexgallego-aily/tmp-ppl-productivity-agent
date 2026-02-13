"""
plots — Visualizations.

1. plot_global_dashboard  — 4 panels: headcount and n_managers by country and function
2. plot_manager_dashboard — Manager KPIs + M&S site KPIs (when available)
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .config import BENCHMARK_METRICS
from .data import get_latest, get_manager_mns_kpis


# ===================================================================
# Colour palette for M&S KPIs
# ===================================================================
_MNS_COLORS = {
    "ABSENTEEISM": "#e74c3c",
    "SAFETY_PSI": "#f39c12",
    "OTIF": "#2ecc71",
    "DISCARDS": "#9b59b6",
}


# ===================================================================
# 1. GLOBAL DASHBOARD
# ===================================================================

def plot_global_dashboard(df, top_n=20):
    """4 panels: headcount and n_managers by country and by function."""
    latest = get_latest(df)

    by_country = (latest.groupby('geo_code')
                  .agg(headcount=('headcount', 'sum'),
                       n_managers=('manager_code', 'nunique'))
                  .sort_values('headcount', ascending=False)
                  .head(top_n).reset_index())

    by_function = (latest.groupby('primary_function')
                   .agg(headcount=('headcount', 'sum'),
                        n_managers=('manager_code', 'nunique'))
                   .sort_values('headcount', ascending=False)
                   .head(top_n).reset_index())

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Headcount by Country', 'N Managers by Country',
            'Headcount by Function', 'N Managers by Function',
        ),
        vertical_spacing=0.18, horizontal_spacing=0.12,
    )

    fig.add_trace(go.Bar(
        x=by_country['geo_code'], y=by_country['headcount'],
        marker_color='#3498db', text=by_country['headcount'],
        textposition='outside', name='Headcount',
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=by_country['geo_code'], y=by_country['n_managers'],
        marker_color='#e74c3c', text=by_country['n_managers'],
        textposition='outside', name='Managers',
    ), row=1, col=2)

    fig.add_trace(go.Bar(
        x=by_function['primary_function'], y=by_function['headcount'],
        marker_color='#2ecc71', text=by_function['headcount'],
        textposition='outside', name='Headcount',
    ), row=2, col=1)

    fig.add_trace(go.Bar(
        x=by_function['primary_function'], y=by_function['n_managers'],
        marker_color='#f39c12', text=by_function['n_managers'],
        textposition='outside', name='Managers',
    ), row=2, col=2)

    fig.update_layout(
        height=1000, showlegend=False,
        title_text=(f"Global Overview — {latest['month'].iloc[0].strftime('%Y-%m')}"
                    f"<br><sub>{latest['manager_code'].nunique():,} managers | "
                    f"{latest['headcount'].sum():,} headcount | "
                    f"{latest['geo_code'].nunique()} countries</sub>"),
        margin=dict(t=100, b=120, l=70, r=50),
    )
    fig.update_xaxes(tickangle=45, tickfont=dict(size=10))
    return fig


# ===================================================================
# 2. MANAGER DASHBOARD  (People KPIs + M&S overlay)
# ===================================================================

def plot_manager_dashboard(df, manager_code, geo_code=None, mns=None):
    """Manager KPI dashboard with optional M&S site overlay.

    Panels (6 rows × 2 cols = 12 panels):
        Row 1: Team Size              | Attrition Rate
        Row 2: Health Components      | Health Score
        Row 3: Median Age             | Median Salary
        Row 4: Gender Balance         | Racial Diversity
        Row 5: Retention Risk         | Critical Flight Risk
        Row 6: Site M&S KPIs (dual)   | Site Absenteeism vs Attrition

    Row 6 only appears when the manager is in a mappable M&S site
    and `mns` DataFrame is provided.

    Parameters
    ----------
    df  : People DataFrame (enriched with mns_business_unit, mns_cluster)
    manager_code : str
    geo_code : str, optional — filter when manager has multiple geo_codes
    mns : DataFrame, optional — enriched M&S KPIs (from load_all())
    """
    mask = df['manager_code'] == manager_code
    if geo_code:
        mask &= df['geo_code'] == geo_code
    data = df[mask].sort_values('month')
    if len(data) == 0:
        print(f"No data for manager {manager_code}")
        return None

    # --- manager context ---
    mgr_geo = data['geo_code'].iloc[0]
    mgr_cluster = (data['location_cluster'].mode()[0]
                   if len(data['location_cluster'].mode()) > 0 else 'N/A')
    mgr_func = (data['primary_function'].mode()[0]
                if len(data['primary_function'].mode()) > 0 else 'N/A')
    mgr_gbu = (data['primary_gbu_level_1'].mode()[0]
               if len(data['primary_gbu_level_1'].mode()) > 0 else '')

    # --- M&S site KPIs ---
    site_mns = None
    mns_level = None
    if mns is not None:
        site_mns, mns_level = get_manager_mns_kpis(df, mns, manager_code)
    has_mns = site_mns is not None and len(site_mns) > 0

    n_rows = 7 if has_mns else 5

    # --- benchmarks ---
    agg = {m: 'mean' for m in BENCHMARK_METRICS}
    country_bm = (df[df['geo_code'] == mgr_geo]
                  .groupby('month').agg(agg).reset_index())
    site_bm = (df[df['location_cluster'] == mgr_cluster]
               .groupby('month').agg(agg).reset_index()
               if mgr_cluster != 'N/A' else None)
    func_bm = (df[df['primary_function'] == mgr_func]
               .groupby('month').agg(agg).reset_index()
               if mgr_func != 'N/A' else None)

    # --- subplots ---
    titles = [
        'Team Size', 'Attrition Rate',
        'Health Components', 'Health Score',
        'Median Age', 'Median Salary',
        'Gender Balance', 'Racial Diversity',
        'Retention Risk', 'Critical Flight Risk',
    ]
    specs = ([[{"secondary_y": False}] * 2] * 4
             + [[{"type": "scatter"}, {"secondary_y": False}]])

    if has_mns:
        if mns_level == 'cluster':
            cl_raw = site_mns['cluster_label'].iloc[0]
            mns_scope_display = (cl_raw.replace(' (Cluster)', '') if isinstance(cl_raw, str) else 'Site')
        else:
            mns_scope_display = site_mns['business_unit_label'].iloc[0] + ' (BU avg)'
        titles += [
            f'M&S Absenteeism — {mns_scope_display}',
            f'M&S Safety PSI — {mns_scope_display}',
            f'M&S OTIF — {mns_scope_display}',
            f'M&S Discards — {mns_scope_display}',
        ]
        specs += [
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
        ]

    fig = make_subplots(
        rows=n_rows, cols=2,
        subplot_titles=titles,
        specs=specs,
        vertical_spacing=0.06, horizontal_spacing=0.12,
    )

    # --- legend dedup ---
    shown = set()

    def _add(td, row, col, secondary_y=False):
        key = td.get('legendgroup', td.get('name'))
        td['showlegend'] = key not in shown
        if key not in shown:
            shown.add(key)
        fig.add_trace(go.Scatter(**td), row=row, col=col, secondary_y=secondary_y)

    def _with_benchmarks(metric, row, col, color, fill=False):
        tc = {'x': data['month'], 'y': data[metric], 'name': 'Manager',
              'line': dict(color=color, width=3), 'mode': 'lines+markers',
              'legendgroup': 'mgr'}
        if fill:
            r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
            tc.update(fill='tozeroy', fillcolor=f'rgba({r},{g},{b},0.2)')
        _add(tc, row, col)
        _add({'x': country_bm['month'], 'y': country_bm[metric],
              'name': f'Country ({mgr_geo})',
              'line': dict(color='orange', width=2, dash='dash'),
              'mode': 'lines', 'legendgroup': 'country'}, row, col)
        if site_bm is not None:
            _add({'x': site_bm['month'], 'y': site_bm[metric],
                  'name': f'Site ({mgr_cluster[:20]})',
                  'line': dict(color='lightblue', width=2, dash='dot'),
                  'mode': 'lines', 'legendgroup': 'site'}, row, col)
        if func_bm is not None:
            _add({'x': func_bm['month'], 'y': func_bm[metric],
                  'name': f'Function ({mgr_func[:20]})',
                  'line': dict(color='lightgreen', width=2, dash='dashdot'),
                  'mode': 'lines', 'legendgroup': 'func'}, row, col)

    # ── Row 1 ──────────────────────────────────────────────────────
    # P1 — Team Size
    fig.add_trace(go.Scatter(
        x=data['month'], y=data['headcount'], name='Headcount',
        line=dict(color='#3498db', width=3),
        fill='tozeroy', fillcolor='rgba(52,152,219,0.2)',
        mode='lines+markers',
    ), row=1, col=1)

    # P2 — Attrition
    _with_benchmarks('attrition_rate_12m_pct', 1, 2, '#e74c3c')

    # ── Row 2 ──────────────────────────────────────────────────────
    # P3 — Health components
    for m, c, n in [('development_score', '#3498db', 'Development'),
                     ('mobility_score', '#e74c3c', 'Mobility'),
                     ('succession_score', '#f39c12', 'Succession')]:
        fig.add_trace(go.Scatter(
            x=data['month'], y=data[m], name=n,
            line=dict(color=c, width=2.5), mode='lines+markers',
            legendgroup='health_comp',
        ), row=2, col=1)
    fig.add_hline(y=80, line_dash="dot", line_color="green", row=2, col=1)
    fig.add_hline(y=60, line_dash="dot", line_color="orange", row=2, col=1)

    # P4 — Health composite
    _with_benchmarks('team_health_score', 2, 2, '#1abc9c', fill=True)
    fig.add_hrect(y0=80, y1=100, fillcolor="green", opacity=0.08, row=2, col=2, layer="below")
    fig.add_hrect(y0=60, y1=80, fillcolor="yellow", opacity=0.08, row=2, col=2, layer="below")
    fig.add_hrect(y0=0, y1=60, fillcolor="red", opacity=0.08, row=2, col=2, layer="below")

    # ── Row 3 ──────────────────────────────────────────────────────
    # P5 — Age
    _with_benchmarks('median_age', 3, 1, '#2ecc71')

    # P6 — Salary
    if data['median_salary'].notna().sum() > 0:
        _with_benchmarks('median_salary', 3, 2, '#9b59b6')
    else:
        fig.add_annotation(
            text="Salary data not available",
            xref="x domain", yref="y domain", x=0.5, y=0.5,
            showarrow=False, font=dict(size=14, color="gray"), row=3, col=2)

    # ── Row 4 ──────────────────────────────────────────────────────
    # P7 — Gender
    _with_benchmarks('pct_female', 4, 1, '#9b59b6')
    fig.add_hline(y=50, line_dash="dot", line_color="gray", row=4, col=1)

    # P8 — Racial diversity
    _with_benchmarks('race_diversity_index', 4, 2, '#f39c12')

    # ── Row 5 ──────────────────────────────────────────────────────
    # P9 — Retention risk (stacked)
    for col_name, name, fc, lg in [
        ('pct_high_retention_risk', 'High Risk', 'rgba(255,0,0,0.6)', 'ret_h'),
        ('pct_medium_retention_risk', 'Med Risk', 'rgba(255,165,0,0.6)', 'ret_m'),
        ('pct_low_retention_risk', 'Low Risk', 'rgba(0,255,0,0.6)', 'ret_l'),
    ]:
        _add({'x': data['month'], 'y': data[col_name], 'name': name,
              'line': dict(width=0), 'stackgroup': 'retention',
              'fillcolor': fc, 'legendgroup': lg}, 5, 1)

    # P10 — Critical flight risk
    _with_benchmarks('pct_critical_flight_risk', 5, 2, '#c0392b', fill=True)
    fig.add_hline(y=10, line_dash="dot", line_color="red", row=5, col=2)

    # ── Rows 6-7 — M&S site KPIs, one per panel (conditional) ─────
    if has_mns:
        _MNS_PANEL_MAP = [
            ("ABSENTEEISM", 6, 1, "#e74c3c", "Absenteeism Rate"),
            ("SAFETY_PSI",  6, 2, "#f39c12", "Safety PSI"),
            ("OTIF",        7, 1, "#2ecc71", "OTIF"),
            ("DISCARDS",    7, 2, "#9b59b6", "Discards Rate"),
        ]
        for kpi, row, col, color, ylabel in _MNS_PANEL_MAP:
            kpi_data = site_mns[site_mns['kpi_code'] == kpi].sort_values('kpi_facts_date')
            if len(kpi_data) == 0:
                fig.add_annotation(
                    text=f"No {kpi} data", xref="x domain", yref="y domain",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=14, color="gray"), row=row, col=col)
                continue
            # Actual value
            _add({'x': kpi_data['kpi_facts_date'], 'y': kpi_data['kpi_value'],
                  'name': f'M&S {kpi}', 'mode': 'lines+markers',
                  'line': dict(color=color, width=2.5),
                  'fill': 'tozeroy', 'fillcolor': f'rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.15)',
                  'legendgroup': f'mns_{kpi}'}, row, col)
            # Target line
            if kpi_data['target_value'].notna().any():
                fig.add_trace(go.Scatter(
                    x=kpi_data['kpi_facts_date'], y=kpi_data['target_value'],
                    name=f'{kpi} target', mode='lines',
                    line=dict(color=color, width=1.5, dash='dot'),
                    showlegend=False,
                ), row=row, col=col)
            fig.update_yaxes(title_text=ylabel, row=row, col=col)

    # --- y-axis labels ---
    axes = [("Headcount", None), ("Attrition %", None),
            ("Score (0-100)", [0, 100]), ("Health (0-100)", [0, 100]),
            ("Age", None), ("Salary", None),
            ("% Female", [0, 100]), ("Diversity Index", None),
            ("% of Team", [0, 100]), ("Flight Risk %", None)]
    for i, (title, rng) in enumerate(axes):
        row, col = divmod(i, 2)
        kw = dict(title_text=title, row=row + 1, col=col + 1)
        if rng:
            kw['range'] = rng
        fig.update_yaxes(**kw)

    # --- layout ---
    mns_label = ""
    if has_mns:
        mns_label = f" | M&S: {mns_scope_display}"

    fig.update_layout(
        height=350 * n_rows,
        title_text=(f"Manager: {manager_code[:40]}<br>"
                    f"<sub>{mgr_geo} | {mgr_cluster} | {mgr_func} | {mgr_gbu}{mns_label}</sub>"),
        showlegend=True,
        legend=dict(x=1.02, y=0.98, xanchor='left', yanchor='top',
                    bgcolor='rgba(255,255,255,0.9)', font=dict(size=9)),
        hovermode='x unified',
        margin=dict(r=280, t=120, b=60, l=60),
    )

    return fig
