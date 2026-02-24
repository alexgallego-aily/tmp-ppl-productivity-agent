"""
src — Manager Team Analytics.

Functions here serve as building blocks for:
  - Interactive CLI (main.py)
  - Future MCP tools (aily-mcp-ppl)
"""

# ── Data loading & analysis ──────────────────────────────────────
from .data import (
    find_manager,
    list_managers,
    get_manager_profile,
    get_available_mns_clusters,
    load_manager_team_kpis,
    get_manager_summary,
    load_manager_domain_kpis,
    get_domain_summary,
    resolve_business_unit,
    aggregate_team_kpis,
    apply_team_size_filter,
    prepare_rca_data,
    run_correlation,
    compute_hierarchical_diversity_column,
    hierarchical_diversity_index,
    HIERARCHICAL_LEVEL_VALUES,
    MIN_TEAM_HEADCOUNT,
    DEFAULT_DOMAIN_LOOKBACK_YEARS,
)

# ── Plotly visualizations ────────────────────────────────────────
from .plots import (
    plot_manager_team_dashboard,
    plot_domain_kpi_dashboard,
    plot_correlation_pair,
)

# ── Config & mapping rules ───────────────────────────────────────
from .config import (
    TEAM_KPI_PANELS,
    TEAM_PALETTE,
    KPI_MAPPING_LABELS,
    KPI_MAPPING_RULES,
    PPL_CORRELATABLE_KPIS,
    suggest_kpi_mapping,
)
