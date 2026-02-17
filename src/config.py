"""Constants and configuration."""

# ── KPI mapping: manager profile → domain KPI code ────────────────
# Rules are tested in order; first match wins.
# Each rule: (keyword_to_search_in_profile, kpi_mapping_code)
KPI_MAPPING_RULES: list[tuple[str, str]] = [
    ("general medicines", "MSLT_GENERAL_MEDICINE"),
    ("general medicine",  "MSLT_GENERAL_MEDICINE"),
    ("genmed",            "MSLT_GENERAL_MEDICINE"),
    ("vaccines",          "MSLT_VACCINES"),
    ("vaccine",           "MSLT_VACCINES"),
    ("specialty care",    "MSLT_SPECIALTY_CARE"),
    ("speciality care",   "MSLT_SPECIALTY_CARE"),
]

# Reverse: MSLT code → human-readable name
KPI_MAPPING_LABELS: dict[str, str] = {
    "MSLT_GENERAL_MEDICINE": "General Medicine",
    "MSLT_VACCINES":         "Vaccines",
    "MSLT_SPECIALTY_CARE":   "Specialty Care",
}


def suggest_kpi_mapping(**fields: str) -> str | None:
    """Suggest a kpi_mapping code from available profile fields.

    Accepts any keyword arguments (gbu_level_1, gbu_level_2,
    gbu_level_3, level_02, level_03, level_04, job_unit, etc.).
    All values are concatenated and scanned for known keywords.
    Returns the first match, or None if nothing matches.
    """
    text = " ".join(str(v) for v in fields.values() if v).lower()
    for keyword, mapping in KPI_MAPPING_RULES:
        if keyword in text:
            return mapping
    return None


# ── Per-team KPI panel definitions ─────────────────────────────────
# (column_name, panel_title, y_axis_range, fill_when_single_team)
TEAM_KPI_PANELS: list[tuple[str, str, list[int] | None, bool]] = [
    # Row 1 — Size & Attrition
    ('headcount',                    'Headcount',                    None,      True),
    ('attrition_rate_pct',           'Attrition Rate 12m (%)',       None,      False),
    # Row 2 — Demographics
    ('avg_age',                      'Average Age',                  None,      False),
    ('pct_near_retirement',          'Near Retirement (≥55) %',      [0, 100],  False),
    # Row 3 — Tenure & Stability
    ('avg_tenure_years',             'Average Tenure (years)',        None,      False),
    ('avg_time_in_position_years',   'Avg Time in Position (years)', None,      False),
    # Row 4 — Compensation & Diversity
    ('median_salary',                'Median Salary',                None,      False),
    ('pct_female',                   '% Female',                     [0, 100],  False),
    # Row 5 — Health
    ('team_health_score',            'Team Health Score',            [0, 100],  True),
    ('development_score',            'Development Score',            [0, 100],  False),
    # Row 6 — Health (cont.)
    ('mobility_score',               'Mobility Score',               [0, 100],  False),
    ('succession_score',             'Succession Score',             [0, 100],  False),
    # Row 7 — Talent Pipeline
    ('pct_ready_for_promotion',      'Ready for Promotion (%)',      [0, 100],  False),
    ('pct_succession_candidates',    'Succession Candidates (%)',    [0, 100],  False),
    # Row 8 — Risk
    ('pct_high_retention_risk',      'High Retention Risk (%)',      [0, 100],  False),
    ('pct_critical_flight_risk',     'Critical Flight Risk (%)',     [0, 100],  False),
    # Row 9 — Management Structure
    ('pct_managers',                 'Managers in Team (%)',         [0, 100],  False),
    ('avg_span_of_control',          'Avg Span of Control',          None,      False),
]

# ── PPL KPIs available for correlation analysis ────────────────────
# All numeric PPL KPI columns that make sense as time-series signals.
PPL_CORRELATABLE_KPIS: list[str] = [
    "headcount",
    "attrition_rate_pct",
    "avg_age",
    "pct_near_retirement",
    "avg_tenure_years",
    "avg_time_in_position_years",
    "pct_female",
    "team_health_score",
    "development_score",
    "mobility_score",
    "succession_score",
    "pct_ready_for_promotion",
    "pct_succession_candidates",
    "pct_high_retention_risk",
    "pct_critical_flight_risk",
    "pct_managers",
    "avg_span_of_control",
    "pct_long_in_position",
]

# Colour palette for teams (hex, enough for ≤16 teams)
TEAM_PALETTE = [
    '#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3',
    '#a6d854', '#ffd92f', '#e5c494', '#b3b3b3',
    '#1b9e77', '#d95f02', '#7570b3', '#e7298a',
    '#66a61e', '#e6ab02', '#a6761d', '#666666',
]

# Management level composition — column names and display order
MANAGEMENT_LEVEL_COLUMNS: list[tuple[str, str]] = [
    ("pct_exec_comm", "Exec Comm"),
    ("pct_exec_level_1", "Exec Level 1"),
    ("pct_exec_level_2", "Exec Level 2"),
    ("pct_level_1", "Level 1"),
    ("pct_level_2", "Level 2"),
    ("pct_level_3", "Level 3"),
    ("pct_level_4", "Level 4"),
    ("pct_level_5", "Level 5"),
    ("pct_local", "Local"),
]

# Colour per management level (for composition charts)
MANAGEMENT_LEVEL_PALETTE: dict[str, str] = {
    "pct_exec_comm":    "#1f77b4",
    "pct_exec_level_1": "#ff7f0e",
    "pct_exec_level_2": "#2ca02c",
    "pct_level_1":      "#d62728",
    "pct_level_2":      "#9467bd",
    "pct_level_3":      "#8c564b",
    "pct_level_4":      "#e377c2",
    "pct_level_5":      "#7f7f7f",
    "pct_local":        "#bcbd22",
}
