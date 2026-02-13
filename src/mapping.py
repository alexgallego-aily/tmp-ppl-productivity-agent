"""
mapping — M&S KPI ↔ People matching logic.

Provides deterministic mapping tables and functions to enrich both datasets
with a common join key (business_unit + cluster).
"""

import pandas as pd

# -----------------------------------------------------------------------
# 1. BUSINESS UNIT mapping:  primary_gbu_level_1 → mns business_unit_label
# -----------------------------------------------------------------------
_BU_RULES = [
    ("Vaccines",        lambda s: "vaccine" in s.lower()),
    ("General Medicine", lambda s: "general medicine" in s.lower()),
    ("Specialty Care",  lambda s: ("specialty care" in s.lower()
                                   or "speciality care" in s.lower())),
]


def map_business_unit(gbu_level_1: str) -> str | None:
    """Map a primary_gbu_level_1 value to an M&S business_unit_label."""
    if not gbu_level_1 or pd.isna(gbu_level_1):
        return None
    for bu, rule in _BU_RULES:
        if rule(gbu_level_1):
            return bu
    return None


# -----------------------------------------------------------------------
# 2. CLUSTER mapping:  location_cluster / location_country → mns cluster_label
# -----------------------------------------------------------------------
_CLUSTER_GEO_MAP = {
    # M&S cluster_label  →  (people_field, people_value(s))
    "Swiftwater (Cluster)":        ("location_cluster", ["Swiftwater"]),
    "Marcy l'Etoile (Cluster)":    ("location_cluster", ["Marcy-L' Etoile"]),
    "Neuville (Cluster)":          ("location_cluster", ["Neuville-Sur-Saône"]),
    "Toronto (Cluster)":           ("location_cluster", ["Toronto", "Toronto Downtown"]),
    "Val de Reuil (Cluster)":      ("location_cluster", ["Val-De-Reuil"]),
    "Protein Sciences (Cluster)":  ("location_cluster", ["Meriden"]),
    "China":                       ("location_country",  ["CN"]),
}

# Inverted index: (field, value) → cluster_label  (for fast row-level lookup)
_CLUSTER_LOOKUP: dict[tuple[str, str], str] = {}
for _cl, (_field, _vals) in _CLUSTER_GEO_MAP.items():
    for _v in _vals:
        _CLUSTER_LOOKUP[(_field, _v)] = _cl


def map_cluster(location_cluster, location_country) -> str | None:
    """Map a People row's location to an M&S cluster_label (geographic only)."""
    if location_cluster and not (isinstance(location_cluster, float) and pd.isna(location_cluster)):
        hit = _CLUSTER_LOOKUP.get(("location_cluster", str(location_cluster)))
        if hit:
            return hit
    if location_country and not (isinstance(location_country, float) and pd.isna(location_country)):
        hit = _CLUSTER_LOOKUP.get(("location_country", str(location_country)))
        if hit:
            return hit
    return None


# -----------------------------------------------------------------------
# 3. Normalise M&S cluster_label  →  strip " (Cluster)" for display
# -----------------------------------------------------------------------
def normalise_cluster_label(label) -> str:
    """Remove the ' (Cluster)' suffix for cleaner display."""
    if not label or (isinstance(label, float) and pd.isna(label)):
        return label
    return str(label).replace(" (Cluster)", "")


# -----------------------------------------------------------------------
# 4. Enrich dataframes
# -----------------------------------------------------------------------
def enrich_people(df: pd.DataFrame) -> pd.DataFrame:
    """Add mns_business_unit and mns_cluster columns to the People dataframe."""
    df = df.copy()
    df["mns_business_unit"] = df["primary_gbu_level_1"].apply(map_business_unit)
    df["mns_cluster"] = df.apply(
        lambda r: map_cluster(r["location_cluster"], r["location_country"]), axis=1
    )
    return df


def enrich_mns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the M&S KPIs dataframe for joining."""
    df = df.copy()
    df["kpi_facts_date"] = pd.to_datetime(df["kpi_facts_date"])
    df["cluster_display"] = df["cluster_label"].apply(normalise_cluster_label)
    return df


# -----------------------------------------------------------------------
# 5. Build the merged dataset
# -----------------------------------------------------------------------
def merge_mns_people(
    people: pd.DataFrame,
    mns: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge M&S KPIs with People manager data.

    Join levels (highest priority first):
      1. business_unit + cluster + month  (geographic clusters)
      2. business_unit + month            (aggregate / product clusters)
    """
    # --- aggregate People to BU + cluster + month level ---
    ppl_agg = (
        people[people["mns_business_unit"].notna()]
        .groupby(["month", "mns_business_unit", "mns_cluster"])
        .agg(
            headcount=("headcount", "sum"),
            total_fte=("total_fte", "sum"),
            avg_attrition=("attrition_rate_12m_pct", "mean"),
            avg_health_score=("team_health_score", "mean"),
            avg_age=("avg_age", "mean"),
            avg_pct_female=("pct_female", "mean"),
            avg_flight_risk=("pct_critical_flight_risk", "mean"),
            n_managers=("manager_code", "nunique"),
        )
        .reset_index()
    )

    # --- Level 1: BU + cluster + month ---
    mns_with_cluster = mns[mns["cluster_label"].notna()].copy()
    merged_cluster = mns_with_cluster.merge(
        ppl_agg[ppl_agg["mns_cluster"].notna()],
        left_on=["business_unit_label", "cluster_label", "kpi_facts_date"],
        right_on=["mns_business_unit", "mns_cluster", "month"],
        how="left",
    )

    # --- Level 2: BU + month (aggregate rows where cluster_label is empty) ---
    ppl_agg_bu = (
        people[people["mns_business_unit"].notna()]
        .groupby(["month", "mns_business_unit"])
        .agg(
            headcount=("headcount", "sum"),
            total_fte=("total_fte", "sum"),
            avg_attrition=("attrition_rate_12m_pct", "mean"),
            avg_health_score=("team_health_score", "mean"),
            avg_age=("avg_age", "mean"),
            avg_pct_female=("pct_female", "mean"),
            avg_flight_risk=("pct_critical_flight_risk", "mean"),
            n_managers=("manager_code", "nunique"),
        )
        .reset_index()
    )

    mns_no_cluster = mns[mns["cluster_label"].isna()].copy()
    merged_bu = mns_no_cluster.merge(
        ppl_agg_bu,
        left_on=["business_unit_label", "kpi_facts_date"],
        right_on=["mns_business_unit", "month"],
        how="left",
    )

    result = pd.concat([merged_cluster, merged_bu], ignore_index=True)
    return result
