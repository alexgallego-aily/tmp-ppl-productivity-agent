"""
src — Manager Team Analytics.

Functions here serve as building blocks for:
  - Interactive CLI (main.py)
  - Future MCP tools (aily-mcp-ppl)
"""

# ── Data loading & analysis ──────────────────────────────────────
from .data import (
    find_manager,
    get_manager_profile,
    get_available_mns_clusters,
    load_manager_team_kpis,
    get_manager_summary,
    load_manager_domain_kpis,
    get_domain_summary,
    resolve_business_unit,
)

# ── Plotly visualizations ────────────────────────────────────────
from .plots import (
    plot_manager_team_dashboard,
    plot_domain_kpi_dashboard,
)

# ── Config & mapping rules ───────────────────────────────────────
from .config import (
    TEAM_KPI_PANELS,
    TEAM_PALETTE,
    KPI_MAPPING_LABELS,
    KPI_MAPPING_RULES,
    suggest_kpi_mapping,
)
