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

from .config import KPI_MAPPING_LABELS, suggest_kpi_mapping
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
    df = dal.db.fetch_data_as_df(query=query)

    _logger.info("Found %d candidate(s)", len(df))
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

    query = f"""
    SELECT DISTINCT c.cluster_label
    FROM data_normalized.mns_kpi_facts k
    LEFT JOIN data_normalized.mns_business_units b
        ON k.business_unit_code = b.business_unit_code
    LEFT JOIN data_normalized.mns_clusters c
        ON k.cluster_code = c.cluster_code
    WHERE (CASE
        WHEN b.business_unit_label IN ('GENMED', 'General Medicine')
            THEN 'General Medicine'
        ELSE b.business_unit_label
    END) = '{business_unit}'
      AND c.cluster_label IS NOT NULL
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


def load_manager_domain_kpis(
    kpi_mapping: str,
    geo_codes: list[str],
    bu_clusters: list[str] | None = None,
    dal: Dal | None = None,
) -> pd.DataFrame:
    """Load MNS domain KPIs relevant to a manager.

    Returns two categories of KPIs in a single DataFrame:
      1. **Business-unit KPIs** — filtered to ``bu_clusters`` if
         provided, otherwise all clusters for the BU.
      2. **Country Organisation KPIs** — filtered to only the clusters
         that match the manager's ``geo_codes``.

    Args:
        kpi_mapping: Domain code (e.g. ``'MSLT_GENERAL_MEDICINE'``).
        geo_codes: Manager's country codes from PPL
                   (e.g. ``['France', 'Germany', 'Italy']``).
        bu_clusters: Optional list of BU cluster_labels to fetch
                     (e.g. ``['Dupixent', 'China']``).  ``None`` = all.
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

    query = _load_sql("manager_domain_kpis.sql").format(
        business_unit=business_unit,
        bu_cluster_filter=bu_cluster_filter,
        geo_list=geo_list,
    )
    _logger.info(
        "Loading domain KPIs: BU=%s, clusters=%s, geos=%s …",
        business_unit,
        f"{len(bu_clusters)} selected" if bu_clusters else "all",
        geo_codes,
    )
    df = dal.db.fetch_data_as_df(query=query)

    if len(df) == 0:
        _logger.warning("No domain KPIs found for %s / %s", business_unit, geo_codes)
        return df

    df["kpi_facts_date"] = pd.to_datetime(df["kpi_facts_date"])

    # Tag each row with a source category for easy filtering later
    df["source"] = df["business_unit_label"].apply(
        lambda bu: "country_org" if bu.startswith("Country Organisation") else "domain"
    )

    n_domain = df[df["source"] == "domain"].shape[0]
    n_country = df[df["source"] == "country_org"].shape[0]
    n_kpis = df["kpi_code"].nunique()
    n_clusters = df["cluster_label"].nunique()
    _logger.info(
        "Loaded %d rows (%d domain, %d country) — "
        "%d unique KPIs, %d clusters",
        len(df), n_domain, n_country, n_kpis, n_clusters,
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

    return {
        "business_unit": domain["business_unit_label"].iloc[0] if len(domain) > 0 else None,
        "domain_kpi_codes": sorted(domain["kpi_code"].unique().tolist()),
        "domain_clusters": sorted(domain["cluster_label"].dropna().unique().tolist()),
        "country_bus": sorted(country["business_unit_label"].unique().tolist()),
        "country_geos": sorted(country["cluster_label"].dropna().unique().tolist()),
        "country_kpi_codes": sorted(country["kpi_code"].unique().tolist()),
        "date_range": (
            domain_df["kpi_facts_date"].min().strftime("%Y-%m"),
            domain_df["kpi_facts_date"].max().strftime("%Y-%m"),
        ),
    }

