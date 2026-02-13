"""Data loading via Aily Data Access Layer (DAL).

Replaces the previous CSV-based loading with SQL queries executed
through the DAL, following Aily's standard data access patterns.
"""

import logging

import pandas as pd
from aily_data_access_layer.dal import Dal
from aily_py_commons.io.read import read_text

from .mapping import enrich_mns, enrich_people, merge_mns_people
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


def load_data(dal: Dal | None = None) -> pd.DataFrame:
    """Load managers summary data from the database.

    Args:
        dal: Optional Dal instance. Creates a new one if not provided.

    Returns:
        DataFrame with manager-level team KPIs, sorted by manager and month.
    """
    if dal is None:
        dal = Dal()

    query = _load_sql("managers_summary.sql")
    _logger.info("Loading managers summary from database …")
    df = dal.db.fetch_data_as_df(query=query)

    df["month"] = pd.to_datetime(df["month"])
    df = df.sort_values(by=["manager_code", "month"]).reset_index(drop=True)
    return df


def load_mns_kpis(dal: Dal | None = None) -> pd.DataFrame:
    """Load M&S KPIs data from the database.

    Args:
        dal: Optional Dal instance. Creates a new one if not provided.

    Returns:
        DataFrame with Manufacturing & Supply KPI facts.
    """
    if dal is None:
        dal = Dal()

    query = _load_sql("mns_kpis.sql")
    _logger.info("Loading M&S KPIs from database …")
    df = dal.db.fetch_data_as_df(query=query)

    df["kpi_facts_date"] = pd.to_datetime(df["kpi_facts_date"])
    return df


def load_all(dal: Dal | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load, enrich and merge both datasets.

    Args:
        dal: Optional Dal instance. A single instance is shared across
             all queries for connection reuse.

    Returns:
        Tuple of (df, mns, merged):
        - df:     People managers enriched with mns_business_unit & mns_cluster
        - mns:    M&S KPIs enriched with cluster_display
        - merged: Joined dataset at BU+cluster+month level
    """
    if dal is None:
        dal = Dal()

    df = enrich_people(load_data(dal))
    mns = enrich_mns(load_mns_kpis(dal))
    merged = merge_mns_people(df, mns)
    return df, mns, merged


def get_latest(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows for the latest available month."""
    return df[df["month"] == df["month"].max()].copy()


def get_manager_mns_kpis(
    df: pd.DataFrame,
    mns: pd.DataFrame,
    manager_code: str,
) -> tuple[pd.DataFrame | None, str | None]:
    """Get M&S KPIs for a manager's site or BU.

    Resolution order:
      1. BU + geographic cluster  (most specific)
      2. BU only                  (fallback — aggregate M&S KPIs for the BU)

    Args:
        df:           People managers DataFrame (enriched).
        mns:          M&S KPIs DataFrame (enriched).
        manager_code: The manager's unique code.

    Returns:
        Tuple of (mns_df, level) where level is 'cluster' or 'bu',
        or (None, None) if no match at all.
    """
    mgr = df[df["manager_code"] == manager_code]
    if len(mgr) == 0:
        return None, None

    bu = mgr["mns_business_unit"].mode()
    if len(bu) == 0 or pd.isna(bu.iloc[0]):
        return None, None
    bu_val = bu.iloc[0]

    # --- Level 1: BU + cluster ---
    cluster = mgr["mns_cluster"].mode()
    if len(cluster) > 0 and pd.notna(cluster.iloc[0]):
        cluster_val = cluster.iloc[0]
        site_mns = mns[
            (mns["cluster_label"] == cluster_val) & (mns["business_unit_label"] == bu_val)
        ].sort_values("kpi_facts_date")
        if len(site_mns) > 0:
            return site_mns, "cluster"

    # --- Level 2: BU only (aggregate rows where cluster_label is NaN) ---
    bu_mns = mns[
        (mns["cluster_label"].isna()) & (mns["business_unit_label"] == bu_val)
    ].sort_values("kpi_facts_date")
    if len(bu_mns) > 0:
        return bu_mns, "bu"

    return None, None
