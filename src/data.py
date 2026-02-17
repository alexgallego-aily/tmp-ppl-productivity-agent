"""Data loading via Aily Data Access Layer (DAL).

Functions exposed here are the building blocks for both:
  - The interactive CLI (main.py)
  - Future MCP tools (aily-mcp-ppl)
"""

from __future__ import annotations

import logging

import pandas as pd
from aily_data_access_layer.dal import Dal
from aily_py_commons.io.read import read_text

from .config import KPI_MAPPING_LABELS, PPL_CORRELATABLE_KPIS, suggest_kpi_mapping
from .paths import SQL_DIR

_logger = logging.getLogger(__name__)


def _load_sql(filename: str) -> str:
    """Read a SQL query from the sql/ directory.

    Args:
        filename: Name of the .sql file (e.g. 'mns_kpis.sql').

    Returns:
        The SQL query as a string.
    """
    return read_text(SQL_DIR / filename)


MIN_TEAM_HEADCOUNT = 5
"""Minimum headcount for a team to be shown individually.

Teams with fewer than this many people (in the latest month) are hidden
from the per-team dashboard but still count in the aggregate view.
"""


def aggregate_team_kpis(data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-team KPIs into a single all-teams row per month.

    Metrics are weighted by headcount.  Absolute metrics (headcount,
    total_fte, exits_rolling_12m) are summed.

    Returns a DataFrame with ``organization_level_code='ALL'`` and
    ``geo_code='ALL'``.
    """
    if len(data) == 0:
        return data

    # Cast all numeric columns to float to avoid Decimal issues from the DB
    sum_cols = ["headcount", "total_fte", "exits_rolling_12m"]
    kpi_cols = [
        "attrition_rate_pct", "avg_age", "pct_near_retirement",
        "avg_tenure_years", "avg_time_in_position_years", "median_salary",
        "pct_female", "team_health_score", "development_score",
        "mobility_score", "succession_score", "pct_ready_for_promotion",
        "pct_succession_candidates", "pct_high_retention_risk",
        "pct_critical_flight_risk", "pct_managers", "avg_span_of_control",
        "pct_long_in_position",
        # Management level composition
        "pct_exec_comm", "pct_exec_level_1", "pct_exec_level_2",
        "pct_level_1", "pct_level_2", "pct_level_3",
        "pct_level_4", "pct_level_5", "pct_local",
    ]
    sum_cols = [c for c in sum_cols if c in data.columns]
    kpi_cols = [c for c in kpi_cols if c in data.columns]

    data = data.copy()
    for c in sum_cols + kpi_cols:
        data[c] = pd.to_numeric(data[c], errors="coerce")

    rows = []
    for month, grp in data.groupby("month"):
        total_hc = float(grp["headcount"].sum())
        row: dict = {"month": month, "organization_level_code": "ALL", "geo_code": "ALL"}

        # Sum columns
        for c in sum_cols:
            row[c] = float(grp[c].sum())

        # Headcount-weighted average for KPI columns
        for c in kpi_cols:
            valid = grp[[c, "headcount"]].dropna(subset=[c])
            if len(valid) == 0 or total_hc == 0:
                row[c] = None
            else:
                row[c] = round(float((valid[c] * valid["headcount"]).sum()) / total_hc, 2)

        # Recompute attrition from aggregated values
        if "exits_rolling_12m" in row and total_hc > 0:
            row["attrition_rate_pct"] = round(row["exits_rolling_12m"] * 100.0 / total_hc, 1)

        # Text columns: take mode
        for tc in ["primary_function", "primary_mgmt_level", "currency"]:
            if tc in grp.columns:
                mode = grp[tc].mode()
                row[tc] = mode.iloc[0] if len(mode) > 0 else None

        rows.append(row)

    result = pd.DataFrame(rows)
    result["month"] = pd.to_datetime(result["month"])
    return result.sort_values("month").reset_index(drop=True)


def apply_team_size_filter(
    data: pd.DataFrame,
    min_headcount: int = MIN_TEAM_HEADCOUNT,
) -> pd.DataFrame:
    """Filter teams by size and prepend an aggregate row.

    Logic:
      1. Always include an aggregate row (``ALL / ALL``) that sums all
         teams weighted by headcount.
      2. Additionally include individual teams whose headcount in the
         **latest month** is >= ``min_headcount``.
      3. Teams below the threshold only contribute to the aggregate.

    Args:
        data: Per-team DataFrame from ``load_manager_team_kpis()``.
        min_headcount: Minimum headcount to show a team individually.

    Returns:
        Combined DataFrame: aggregate rows first, then qualifying teams.
        Includes a ``'team_label'`` column for display (``'ALL'`` or
        ``'<org_level> · <geo>'``).
    """
    if len(data) == 0:
        return data

    # 1. Aggregate all teams
    agg = aggregate_team_kpis(data)
    agg["team_label"] = "ALL"

    # 2. Find teams with enough headcount in the latest month
    latest_month = data["month"].max()
    latest = data[data["month"] == latest_month]
    qualifying = latest[latest["headcount"] >= min_headcount][
        ["organization_level_code", "geo_code"]
    ].drop_duplicates()

    if len(qualifying) == 0:
        return agg

    # 3. Keep full history for qualifying teams
    team_data = data.merge(
        qualifying, on=["organization_level_code", "geo_code"], how="inner",
    ).copy()
    team_data["team_label"] = (
        team_data["organization_level_code"] + " · " + team_data["geo_code"]
    )

    # 4. Combine: aggregate first, then individual teams
    combined = pd.concat([agg, team_data], ignore_index=True)
    combined["month"] = pd.to_datetime(combined["month"])
    return combined.sort_values(
        ["team_label", "organization_level_code", "geo_code", "month"]
    ).reset_index(drop=True)


# =====================================================================
# Per-manager team KPIs (primary flow)
# =====================================================================

def load_manager_team_kpis(
    manager_code: str,
    dal: Dal | None = None,
) -> pd.DataFrame:
    """Load per-team monthly KPIs for a specific manager.

    Executes ``manager_team_kpis.sql`` parameterised with the given
    manager code.  Returns one row per (month, organization_level_code,
    geo_code) with ~20 KPIs.

    Args:
        manager_code: The manager's hashed employee code.
        dal: Optional Dal instance.  Creates a new one if not provided.

    Returns:
        DataFrame sorted by organization_level_code, geo_code, month.
    """
    if dal is None:
        dal = Dal()

    query = _load_sql("manager_team_kpis.sql").format(manager_code=manager_code)
    _logger.info("Loading team KPIs for manager %s …", manager_code[:16])
    df = dal.db.fetch_data_as_df(query=query)

    if len(df) == 0:
        _logger.warning("No data returned for manager %s", manager_code[:16])
        return df

    df["month"] = pd.to_datetime(df["month"])

    # Months where ppl_positions hasn't synced yet produce NULL org_level.
    # Drop those rows — they'd break the team grouping.
    n_before = len(df)
    df = df.dropna(subset=["organization_level_code"])
    n_dropped = n_before - len(df)
    if n_dropped:
        _logger.info(
            "Dropped %d rows with NULL organization_level_code "
            "(positions table not yet synced for those months)",
            n_dropped,
        )

    return (
        df.sort_values(by=["organization_level_code", "geo_code", "month"])
        .reset_index(drop=True)
    )


# =====================================================================
# Manager lookup / resolution
# =====================================================================

def find_manager(
    *,
    management_level: str | None = None,
    geo_code: str | None = None,
    location_contains: str | None = None,
    gbu_contains: str | None = None,
    level_02_contains: str | None = None,
    function_contains: str | None = None,
    is_manager_only: bool = True,
    dal: Dal | None = None,
) -> pd.DataFrame:
    """Search for manager candidates using descriptive filters.

    Builds a WHERE clause dynamically from the provided keyword
    arguments and executes ``find_manager.sql``.  Useful as an
    MCP tool for agent-based manager resolution.

    All ``*_contains`` filters use case-insensitive ILIKE matching.

    Args:
        management_level: Exact match (e.g. 'Exec Level 2', 'Level 3').
        geo_code: Exact country match (e.g. 'Germany', 'France').
        location_contains: Substring in the Location field from extra JSON.
        gbu_contains: Substring in GBU_Level_1 from extra JSON.
        level_02_contains: Substring in Level_02_From_Top from extra JSON.
        function_contains: Substring in job_unit_code.
        is_manager_only: If True, only return employees who are managers.
        dal: Optional Dal instance.

    Returns:
        DataFrame of candidate employees with descriptive context,
        ordered by employees_managed DESC.
    """
    if dal is None:
        dal = Dal()

    clauses: list[str] = []

    if management_level:
        clauses.append(f"AND management_level_code = '{management_level}'")
    if geo_code:
        clauses.append(f"AND geo_code = '{geo_code}'")
    if location_contains:
        clauses.append(
            f"AND SPLIT_PART(SPLIT_PART(extra::TEXT, '\"Location\": \"', 2), '\"', 1) "
            f"ILIKE '%{location_contains}%'"
        )
    if gbu_contains:
        clauses.append(
            f"AND SPLIT_PART(SPLIT_PART(extra::TEXT, '\"GBU_Level_1\": \"', 2), '\"', 1) "
            f"ILIKE '%{gbu_contains}%'"
        )
    if level_02_contains:
        clauses.append(
            f"AND SPLIT_PART(SPLIT_PART(extra::TEXT, '\"Level_02_From_Top\": \"', 2), '\"', 1) "
            f"ILIKE '%{level_02_contains}%'"
        )
    if function_contains:
        clauses.append(f"AND job_unit_code ILIKE '%{function_contains}%'")
    if is_manager_only:
        clauses.append("AND is_manager = TRUE")

    where_clause = "\n  ".join(clauses) if clauses else ""

    query = _load_sql("find_manager.sql").format(where_clause=where_clause)
    _logger.info("Searching for manager candidates …")
    _logger.debug("WHERE clause: %s", where_clause)
    df = dal.db.fetch_data_as_df(query=query)

    _logger.info("Found %d candidate(s)%s",
                 len(df),
                 " (is_manager=TRUE filter active)" if is_manager_only else "")

    # Hint: if few results with is_manager filter, count how many without
    if is_manager_only and len(df) <= 3:
        without_mgr = [c for c in clauses if "is_manager" not in c]
        where_no_mgr = "\n  ".join(without_mgr) if without_mgr else ""
        q2 = _load_sql("find_manager.sql").format(where_clause=where_no_mgr)
        df2 = dal.db.fetch_data_as_df(query=q2)
        extra = len(df2) - len(df)
        if extra > 0:
            _logger.info(
                "Hint: %d more candidate(s) if you use --include-non-managers",
                extra,
            )

    return df


def get_manager_summary(data: pd.DataFrame) -> dict:
    """Derive a summary dict from per-team KPIs.

    Uses ALL months for teams/geos (matching the dashboard view) and
    the latest month for headcount/fte snapshots.

    Useful for titles, context labels, and agent metadata.

    Args:
        data: DataFrame returned by ``load_manager_team_kpis()``.

    Returns:
        Dict with keys: n_teams, geos, functions, total_headcount,
        total_fte, months_range, latest_month.
    """
    if len(data) == 0:
        return {}

    latest = data[data["month"] == data["month"].max()]

    # Teams and geos across ALL months (consistent with the dashboard,
    # which plots all unique org_level × geo combinations over time).
    all_teams = data.groupby(["organization_level_code", "geo_code"]).size()
    all_geos = sorted(data["geo_code"].dropna().unique().tolist())
    all_functions = sorted(data["primary_function"].dropna().unique().tolist())

    return {
        "n_teams": len(all_teams),
        "geos": all_geos,
        "functions": all_functions,
        "total_headcount": int(latest["headcount"].sum()),
        "total_fte": round(float(latest["total_fte"].sum()), 1),
        "months_range": (
            data["month"].min().strftime("%Y-%m"),
            data["month"].max().strftime("%Y-%m"),
        ),
        "latest_month": data["month"].max().strftime("%Y-%m"),
    }


# =====================================================================
# Manager profile (lightweight pre-flight)
# =====================================================================

def get_manager_profile(
    manager_code: str,
    dal: Dal | None = None,
) -> dict | None:
    """Lightweight profile of a manager: own info + active teams.

    Two-phase approach:
      1. ``manager_profile.sql`` — manager's self record + aggregated
         context from direct reports (GBU, L2, L3) for kpi_mapping.
      2. ``manager_active_teams.sql`` — org_level × geo breakdown.
         Falls back up to 3 months if positions data not synced.

    Args:
        manager_code: The manager's hashed employee code.
        dal: Optional Dal instance.

    Returns:
        Profile dict, or None if manager not found.
    """
    if dal is None:
        dal = Dal()

    # ── Phase 1: self info + report context ──────────────────────
    query = _load_sql("manager_profile.sql").format(manager_code=manager_code)
    _logger.info("Loading profile for manager %s …", manager_code[:16])
    df = dal.db.fetch_data_as_df(query=query)

    if len(df) == 0:
        _logger.warning("Manager %s not found in latest snapshot", manager_code[:16])
        return None

    s = df.iloc[0]

    # ── Phase 2: active teams (with snapshot fallback) ───────────
    active_teams, team_snap_used = _load_active_teams(manager_code, dal)

    geo_codes = sorted({t["geo"] for t in active_teams if t.get("geo")})

    # ── Derive kpi_mapping from ALL available text ───────────────
    # Pass every GBU + Level_From_Top field (self + reports) so the
    # keyword scan catches the domain regardless of which field is populated.
    kpi_suggestion = suggest_kpi_mapping(
        gbu_level_1=_s(s, "gbu_level_1"),
        reports_gbu_1=_s(s, "reports_gbu_level_1"),
        reports_gbu_2=_s(s, "reports_gbu_level_2"),
        reports_gbu_3=_s(s, "reports_gbu_level_3"),
        level_02=_s(s, "level_02_from_top"),
        level_03=_s(s, "level_03_from_top"),
        reports_level_02=_s(s, "reports_level_02"),
        reports_level_03=_s(s, "reports_level_03"),
        reports_level_04=_s(s, "reports_level_04"),
        job_unit=_s(s, "primary_function"),
    )

    return {
        "employee_code": s["employee_code"],
        "management_level": _s(s, "management_level_code"),
        "geo_code": _s(s, "geo_code"),
        "location": _s(s, "location"),
        "gbu_level_1": _s(s, "gbu_level_1"),
        "level_02_from_top": _s(s, "level_02_from_top"),
        "level_03_from_top": _s(s, "level_03_from_top"),
        "reports_gbu_level_1": _s(s, "reports_gbu_level_1"),
        "reports_gbu_level_2": _s(s, "reports_gbu_level_2"),
        "reports_gbu_level_3": _s(s, "reports_gbu_level_3"),
        "reports_level_02": _s(s, "reports_level_02"),
        "reports_level_03": _s(s, "reports_level_03"),
        "reports_level_04": _s(s, "reports_level_04"),
        "primary_function": _s(s, "primary_function"),
        "is_manager": bool(s.get("is_manager", False)),
        "employees_managed": int(s.get("employees_managed", 0)),
        "direct_report_count": int(s.get("direct_report_count", 0)),
        "active_teams": active_teams,
        "geo_codes": geo_codes,
        "team_snapshot": team_snap_used,
        "kpi_mapping": kpi_suggestion,
        "kpi_mapping_label": (
            KPI_MAPPING_LABELS.get(kpi_suggestion, "")
            if kpi_suggestion else ""
        ),
    }


def _s(row, col: str) -> str:
    """Safely extract a string field from a DataFrame row."""
    val = row.get(col, "")
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val)


def _load_active_teams(
    manager_code: str,
    dal: Dal,
) -> tuple[list[dict], str]:
    """Load active teams with snapshot fallback.

    Tries the latest employee snapshot first; if all org_level_codes
    are NULL (positions not synced), falls back up to 3 months.

    Returns:
        Tuple of (active_teams_list, snapshot_date_used).
    """
    # Get available snapshots (latest 4)
    snaps_df = dal.db.fetch_data_as_df(query=f"""
        SELECT DISTINCT DATE_TRUNC('month', snapshot_date)::DATE AS snap
        FROM data_normalized.ppl_employees
        WHERE manager_code = '{manager_code}'
          AND is_artificial_record = FALSE
        ORDER BY snap DESC
        LIMIT 4
    """)

    if len(snaps_df) == 0:
        return [], ""

    template = _load_sql("manager_active_teams.sql")

    for _, snap_row in snaps_df.iterrows():
        snap = str(snap_row["snap"])
        query = template.format(manager_code=manager_code, snapshot_date=snap)
        teams_df = dal.db.fetch_data_as_df(query=query)

        active_teams = []
        for _, t in teams_df.iterrows():
            org = t.get("organization_level_code")
            if org is not None and pd.notna(org):
                active_teams.append({
                    "org_level": org,
                    "geo": t.get("geo_code", ""),
                    "size": int(t.get("team_size", 0)),
                    "function": _s(t, "primary_function"),
                })

        if active_teams:
            _logger.info(
                "Active teams found at snapshot %s: %d teams",
                snap, len(active_teams),
            )
            return active_teams, snap

        _logger.info("No org_level data at %s, trying earlier snapshot …", snap)

    # All snapshots failed — return geo-only breakdown from latest
    _logger.warning("Could not resolve org_levels; returning geo-only teams")
    latest_snap = str(snaps_df.iloc[0]["snap"])
    query = template.format(manager_code=manager_code, snapshot_date=latest_snap)
    teams_df = dal.db.fetch_data_as_df(query=query)
    fallback = []
    for _, t in teams_df.iterrows():
        fallback.append({
            "org_level": None,
            "geo": t.get("geo_code", ""),
            "size": int(t.get("team_size", 0)),
            "function": _s(t, "primary_function"),
        })
    return fallback, latest_snap


def get_available_mns_clusters(
    business_unit: str,
    dal: Dal | None = None,
) -> list[str]:
    """List available MNS cluster_labels for a given business unit.

    Lightweight query to discover what clusters exist before running
    the heavy domain KPI query.

    Args:
        business_unit: e.g. 'General Medicine', 'Vaccines'.
        dal: Optional Dal instance.

    Returns:
        Sorted list of cluster_label strings.
    """
    if dal is None:
        dal = Dal()

    # EXISTS semi-join: iterates the small clusters table and short-circuits
    # as soon as one matching fact row is found per cluster.
    query = f"""
    SELECT c.cluster_label
    FROM data_normalized.mns_clusters c
    WHERE c.cluster_label IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM data_normalized.mns_kpi_facts k
        WHERE k.cluster_code = c.cluster_code
          AND k.business_unit_code IN (
            SELECT business_unit_code
            FROM data_normalized.mns_business_units
            WHERE (CASE
                WHEN business_unit_label IN ('GENMED', 'General Medicine')
                    THEN 'General Medicine'
                ELSE business_unit_label
            END) = '{business_unit}'
          )
      )
    ORDER BY c.cluster_label
    """
    df = dal.db.fetch_data_as_df(query=query)
    return sorted(df["cluster_label"].tolist())


# =====================================================================
# Domain (MNS) KPIs for a manager
# =====================================================================

# kpi_mapping codes → MNS business_unit_label
_KPI_MAPPING_BU: dict[str, str] = {
    "MSLT_GENERAL_MEDICINE": "General Medicine",
    "MSLT_VACCINES": "Vaccines",
    "MSLT_SPECIALTY_CARE": "Specialty Care",
}


def resolve_business_unit(kpi_mapping: str) -> str:
    """Map a kpi_mapping code to an MNS business_unit_label.

    Falls back to stripping 'MSLT_', replacing '_' with spaces, and
    title-casing if the code isn't in the known mapping dict.
    """
    if kpi_mapping in _KPI_MAPPING_BU:
        return _KPI_MAPPING_BU[kpi_mapping]
    # Fallback: MSLT_GENERAL_MEDICINE → General Medicine
    stripped = kpi_mapping.replace("MSLT_", "").replace("_", " ").title()
    _logger.warning(
        "kpi_mapping '%s' not in known mapping; falling back to '%s'",
        kpi_mapping, stripped,
    )
    return stripped


DEFAULT_DOMAIN_LOOKBACK_YEARS = 3
"""How many years of MNS KPI history to fetch by default."""


def load_manager_domain_kpis(
    kpi_mapping: str,
    geo_codes: list[str],
    bu_clusters: list[str] | None = None,
    lookback_years: int | None = DEFAULT_DOMAIN_LOOKBACK_YEARS,
    dal: Dal | None = None,
) -> pd.DataFrame:
    """Load MNS domain KPIs relevant to a manager.

    Returns three categories of KPIs in a single DataFrame:
      1. **Business-unit KPIs** — filtered to ``bu_clusters`` if
         provided, otherwise all clusters for the BU.
      2. **Country Organisation KPIs** — filtered to only the clusters
         that match the manager's ``geo_codes``.
      3. **BU aggregate KPIs** — all clusters summed per date.

    Args:
        kpi_mapping: Domain code (e.g. ``'MSLT_GENERAL_MEDICINE'``).
        geo_codes: Manager's country codes from PPL
                   (e.g. ``['France', 'Germany', 'Italy']``).
        bu_clusters: Optional list of BU cluster_labels to fetch
                     (e.g. ``['Dupixent', 'China']``).  ``None`` = all.
        lookback_years: How many years of history to fetch.
                        ``None`` = no limit (all history).  Default 3.
        dal: Optional Dal instance.

    Returns:
        DataFrame with columns: kpi_code, business_unit_label,
        cluster_label, kpi_facts_date, kpi_value, target_value, source.
    """
    if dal is None:
        dal = Dal()

    business_unit = resolve_business_unit(kpi_mapping)

    # Build quoted CSVs for the IN clauses (escape single quotes)
    geo_list = ", ".join(f"'{g}'" for g in geo_codes) if geo_codes else "'__NONE__'"

    # Cluster filter: empty string = all clusters, otherwise AND IN (...)
    if bu_clusters:
        escaped = [c.replace("'", "''") for c in bu_clusters]
        bu_cluster_filter = "AND c.cluster_label IN (" + ", ".join(f"'{c}'" for c in escaped) + ")"
    else:
        bu_cluster_filter = ""

    # Date filter: drastically reduces scan on large fact tables
    if lookback_years:
        date_filter = f"AND k.kpi_facts_date >= CURRENT_DATE - INTERVAL '{lookback_years} years'"
    else:
        date_filter = ""

    query = _load_sql("manager_domain_kpis.sql").format(
        business_unit=business_unit,
        bu_cluster_filter=bu_cluster_filter,
        geo_list=geo_list,
        date_filter=date_filter,
    )
    _logger.info(
        "Loading domain KPIs: BU=%s, clusters=%s, geos=%s, lookback=%s …",
        business_unit,
        f"{len(bu_clusters)} selected" if bu_clusters else "all",
        geo_codes,
        f"{lookback_years}y" if lookback_years else "all",
    )
    df = dal.db.fetch_data_as_df(query=query)

    if len(df) == 0:
        _logger.warning("No domain KPIs found for %s / %s", business_unit, geo_codes)
        return df

    df["kpi_facts_date"] = pd.to_datetime(df["kpi_facts_date"])

    # source column now comes from SQL (domain, country_org, bu_aggregate)

    n_domain = df[df["source"] == "domain"].shape[0]
    n_country = df[df["source"] == "country_org"].shape[0]
    n_bu_agg = df[df["source"] == "bu_aggregate"].shape[0]
    n_kpis = df["kpi_code"].nunique()
    n_clusters = df[df["source"] == "domain"]["cluster_label"].nunique()
    _logger.info(
        "Loaded %d rows (%d domain, %d country, %d bu_aggregate) — "
        "%d unique KPIs, %d clusters",
        len(df), n_domain, n_country, n_bu_agg, n_kpis, n_clusters,
    )
    return df.sort_values(
        ["source", "business_unit_label", "kpi_code", "cluster_label", "kpi_facts_date"]
    ).reset_index(drop=True)


def get_domain_summary(domain_df: pd.DataFrame) -> dict:
    """Derive a summary of the loaded domain KPIs.

    Args:
        domain_df: DataFrame from ``load_manager_domain_kpis()``.

    Returns:
        Dict with domain/country breakdowns.
    """
    if len(domain_df) == 0:
        return {}

    domain = domain_df[domain_df["source"] == "domain"]
    country = domain_df[domain_df["source"] == "country_org"]
    bu_agg = domain_df[domain_df["source"] == "bu_aggregate"]

    return {
        "business_unit": domain["business_unit_label"].iloc[0] if len(domain) > 0 else (
            bu_agg["business_unit_label"].iloc[0] if len(bu_agg) > 0 else None
        ),
        "domain_kpi_codes": sorted(domain["kpi_code"].unique().tolist()),
        "domain_clusters": sorted(domain["cluster_label"].dropna().unique().tolist()),
        "country_bus": sorted(country["business_unit_label"].unique().tolist()),
        "country_geos": sorted(country["cluster_label"].dropna().unique().tolist()),
        "country_kpi_codes": sorted(country["kpi_code"].unique().tolist()),
        "bu_aggregate_kpi_codes": sorted(bu_agg["kpi_code"].unique().tolist()),
        "date_range": (
            domain_df["kpi_facts_date"].min().strftime("%Y-%m"),
            domain_df["kpi_facts_date"].max().strftime("%Y-%m"),
        ),
    }


# =====================================================================
# Root Cause Analysis (RCA) — MNS ← PPL correlation
# =====================================================================

def prepare_rca_data(
    ppl_data: pd.DataFrame,
    domain_df: pd.DataFrame,
) -> tuple[dict, dict[str, pd.DataFrame]]:
    """Reshape PPL + MNS KPIs into the format expected by RootCauseAnalysis.

    * **Mother table** (effect): MNS BU-aggregate KPIs — one time series
      per ``kpi_code``.
    * **Node tables** (potential causes): one per PPL KPI — a single
      aggregated time series across all teams.

    Both are normalised to first-of-month dates so they align.

    Args:
        ppl_data: DataFrame from ``load_manager_team_kpis()``.
        domain_df: DataFrame from ``load_manager_domain_kpis()``.

    Returns:
        ``(config_dict, df_dict)`` ready for
        ``RootCauseAnalysis.rca_calculation()``.
    """
    # --- Mother: MNS KPIs (prefer bu_aggregate, fallback to domain) ---
    bu_agg = domain_df[domain_df["source"] == "bu_aggregate"].copy()
    if len(bu_agg) == 0:
        bu_agg = domain_df[domain_df["source"] == "domain"].copy()

    if len(bu_agg) == 0:
        _logger.warning("No MNS KPIs available for RCA")
        return {"mother_table": {"name": "mns"}, "node_tables": {}}, {}

    # Normalise dates to first-of-month
    bu_agg["date_col"] = pd.to_datetime(bu_agg["kpi_facts_date"]).dt.to_period("M").dt.to_timestamp()
    mother_df = (
        bu_agg.rename(columns={"kpi_code": "id_kpi_code", "kpi_value": "value_col"})
        [["id_kpi_code", "date_col", "value_col"]]
        .dropna(subset=["value_col"])
        .drop_duplicates(subset=["id_kpi_code", "date_col"])
        .sort_values(["id_kpi_code", "date_col"])
        .reset_index(drop=True)
    )
    # Dummy link column — the library's filter_dataframe crashes with
    # empty id_link (mask stays as scalar True instead of boolean Series).
    mother_df["id_all"] = "ALL"

    # --- Nodes: aggregated PPL KPIs ---
    agg = aggregate_team_kpis(ppl_data)
    if len(agg) == 0:
        _logger.warning("No PPL KPIs available for RCA")
        return {"mother_table": {"name": "mns"}, "node_tables": {}}, {}

    agg["date_col"] = pd.to_datetime(agg["month"]).dt.to_period("M").dt.to_timestamp()

    ppl_cols = [c for c in PPL_CORRELATABLE_KPIS if c in agg.columns]

    df_dict: dict[str, pd.DataFrame] = {"mns": mother_df}
    node_tables: dict = {}

    for kpi_col in ppl_cols:
        node_df = (
            agg[["date_col", kpi_col]]
            .rename(columns={kpi_col: "value_col"})
            .dropna(subset=["value_col"])
            .sort_values("date_col")
            .reset_index(drop=True)
        )
        # Granger needs at least ~7 observations for meaningful tests
        if len(node_df) < 7:
            continue

        node_df["id_all"] = "ALL"
        df_dict[kpi_col] = node_df
        node_tables[kpi_col] = {
            "name": kpi_col,
            "bridge_df": {"apply": False, "data_paths": {}},
            "id_link": {"id_all": "id_all"},
            "apply_condition": {"name": None, "utils_module": None},
        }

    config = {
        "mother_table": {"name": "mns"},
        "node_tables": node_tables,
    }

    n_mns = mother_df["id_kpi_code"].nunique()
    n_ppl = len(node_tables)
    n_dates = mother_df["date_col"].nunique()
    _logger.info(
        "RCA prepared: %d MNS KPIs × %d PPL KPIs, %d months",
        n_mns, n_ppl, n_dates,
    )
    return config, df_dict


def run_correlation(
    ppl_data: pd.DataFrame,
    domain_df: pd.DataFrame,
    max_lag: int = 6,
) -> pd.DataFrame:
    """Run Root Cause Analysis: MNS KPIs (effect) vs PPL KPIs (cause).

    Wraps ``aily_ai_correlator.root_cause.RootCauseAnalysis`` using
    BU-aggregate MNS KPIs as the mother (effect) and aggregated PPL KPIs
    as node tables (potential causes).

    Only pairs that pass Granger Causality and/or Transfer Entropy are
    returned.

    Args:
        ppl_data: DataFrame from ``load_manager_team_kpis()``.
        domain_df: DataFrame from ``load_manager_domain_kpis()``.
        max_lag: Maximum lag (months) for the Granger test.  Default 6.

    Returns:
        DataFrame with columns: id_kpi_code (MNS), kpi (PPL KPI name),
        min_p_value, transfer_entropy, explained_entropy, lag.
        Empty DataFrame if no significant correlations found.
    """
    from aily_ai_correlator.root_cause.root_cause_analysis import RootCauseAnalysis

    config, df_dict = prepare_rca_data(ppl_data, domain_df)

    if not config["node_tables"]:
        _logger.warning("No PPL KPIs eligible for RCA — not enough data points")
        return pd.DataFrame()

    _logger.info(
        "Running RCA: %d node KPIs, max_lag=%d …",
        len(config["node_tables"]), max_lag,
    )
    rca = RootCauseAnalysis(conf_dict=config, max_lag_permitted=max_lag)
    result = rca.rca_calculation(df_dict=df_dict)

    if len(result) > 0:
        # Sort by strongest signal (lowest p-value, highest entropy)
        result = (
            result
            .sort_values(["id_kpi_code", "min_p_value", "explained_entropy"],
                         ascending=[True, True, False])
            .reset_index(drop=True)
        )
        _logger.info("RCA found %d significant correlations", len(result))
    else:
        _logger.info("RCA: no significant correlations found")

    return result

