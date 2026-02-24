"""CausalImpact analysis for manager hire events on MNS KPIs.

This module provides functions to analyze the causal effect of a manager
being hired in a new organization on MNS (M&S) KPI time series.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from aily_data_access_layer.dal import Dal

from .data import load_manager_domain_kpis, resolve_business_unit
from .paths import SQL_DIR

_logger = logging.getLogger(__name__)


def get_manager_hire_dates(
    manager_code: str,
    organization_level_code: str | None = None,
    dal: Dal | None = None,
) -> pd.DataFrame:
    """Get hire dates for employees managed by a specific manager.
    
    Uses the provided SQL query to find when employees were hired
    in a specific organization_level_code under a manager.
    
    Args:
        manager_code: The manager's hashed employee code.
        organization_level_code: Optional filter for specific org level.
        dal: Optional Dal instance.
        
    Returns:
        DataFrame with columns: employee_code, organization_level_code,
        manager_code, snapshot_date (hire date).
    """
    if dal is None:
        dal = Dal()
    
    # Build the query with optional org level filter
    org_filter = ""
    if organization_level_code:
        org_filter = f"AND pp.organization_level_code = '{organization_level_code}'"
    
    query = f"""
    SELECT
        pe.employee_code,
        pp.organization_level_code,
        pe.manager_code,
        pe.snapshot_date
    FROM data_normalized.ppl_moves pm 
    LEFT JOIN data_normalized.ppl_employees pe
        ON pm.employee_code = pe.employee_code 
        AND pm.snapshot_date = pe.snapshot_date 
    LEFT JOIN data_normalized.ppl_positions pp
        ON pe.position_code = pp.position_code
        AND pe.snapshot_date = pp.snapshot_date
    WHERE pm.move_type = 'Hire'
      AND pe.manager_code = '{manager_code}'
      {org_filter}
    ORDER BY pe.snapshot_date
    """
    
    _logger.info("Loading hire dates for manager %s...", manager_code[:16])
    df = dal.db.fetch_data_as_df(query=query)
    
    if len(df) > 0:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
        
        # Filter: only include hire events at least 1 year before today
        # This ensures we have enough historical data and avoids future dates
        cutoff_date = pd.Timestamp.now() - pd.DateOffset(years=1)
        n_before = len(df)
        df = df[df["snapshot_date"] < cutoff_date].copy()
        n_filtered = n_before - len(df)
        
        if n_filtered > 0:
            _logger.info(
                "Filtered out %d hire event(s) within the last year (need at least 1 year of history)",
                n_filtered
            )
        
        if len(df) > 0:
            _logger.info("Found %d eligible hire events (at least 1 year old)", len(df))
        else:
            _logger.warning(
                "No eligible hire events found (all are within the last year). "
                "Need events at least 1 year old for analysis."
            )
    else:
        _logger.warning("No hire events found for manager %s", manager_code[:16])
    
    return df


def prepare_causalimpact_data(
    domain_df: pd.DataFrame,
    intervention_date: datetime,
    pre_period_months: int = 12,
    post_period_months: int = 12,
    kpi_code: str | None = None,
    use_bu_aggregate: bool = True,
) -> tuple[pd.DataFrame, tuple[datetime, datetime], tuple[datetime, datetime]]:
    """Prepare MNS KPI data for CausalImpact analysis.
    
    Prepares time series data with:
    - Response variable: Target KPI (affected by manager hire)
    - Control variables: Other KPIs or clusters (unaffected)
    
    Args:
        domain_df: DataFrame from load_manager_domain_kpis().
        intervention_date: Date when manager was hired (intervention point).
        pre_period_months: Months before intervention for training. Default 12.
        post_period_months: Months after intervention for analysis. Default 12.
        kpi_code: Specific KPI to analyze. If None, uses first available.
        use_bu_aggregate: If True, use BU aggregate KPIs; else use domain clusters.
        
    Returns:
        Tuple of (data_df, pre_period, post_period) where:
        - data_df: DataFrame with date index and columns for response + controls
        - pre_period: (start_date, intervention_date - 1 day)
        - post_period: (intervention_date, end_date)
    """
    if len(domain_df) == 0:
        raise ValueError("domain_df is empty")
    
    # Filter to BU aggregate or domain clusters
    if use_bu_aggregate:
        source_filter = domain_df["source"] == "bu_aggregate"
    else:
        source_filter = domain_df["source"] == "domain"
    
    filtered_df = domain_df[source_filter].copy()
    
    if len(filtered_df) == 0:
        raise ValueError(f"No {'BU aggregate' if use_bu_aggregate else 'domain'} KPIs found")
    
    # Select KPI if specified, otherwise use first available
    available_kpis = filtered_df["kpi_code"].unique()
    if kpi_code is None:
        kpi_code = available_kpis[0]
        _logger.info("Using first available KPI: %s", kpi_code)
    elif kpi_code not in available_kpis:
        raise ValueError(f"KPI {kpi_code} not found. Available: {available_kpis}")
    
    # Calculate date ranges
    intervention_date = pd.to_datetime(intervention_date)
    pre_start = intervention_date - pd.DateOffset(months=pre_period_months)
    post_end = intervention_date + pd.DateOffset(months=post_period_months)
    
    # Ensure post_end doesn't exceed available data
    # Limit to 1 year before today AND to the maximum date in the data
    max_available_date = pd.Timestamp.now() - pd.DateOffset(days=1)
    max_data_date = filtered_df["kpi_facts_date"].max() if len(filtered_df) > 0 else max_available_date
    max_allowed_date = min(max_available_date, max_data_date)
    
    if post_end > max_allowed_date:
        original_post_end = post_end
        post_end = max_allowed_date
        _logger.info(
            "Adjusted post_period end date from %s to %s (limited by available data)",
            original_post_end.strftime("%Y-%m-%d"),
            post_end.strftime("%Y-%m-%d")
        )
    
    # Filter to date range
    date_mask = (
        (filtered_df["kpi_facts_date"] >= pre_start) &
        (filtered_df["kpi_facts_date"] <= post_end)
    )
    filtered_df = filtered_df[date_mask].copy()
    
    # Ensure we have data for the post period
    if len(filtered_df) == 0:
        raise ValueError(
            f"No data available for the specified date range "
            f"({pre_start.strftime('%Y-%m-%d')} to {post_end.strftime('%Y-%m-%d')})"
        )
    
    # Convert kpi_value to float to avoid Decimal/float type mixing issues
    filtered_df["kpi_value"] = pd.to_numeric(filtered_df["kpi_value"], errors="coerce")
    
    # Prepare response variable (target KPI)
    target_kpi = filtered_df[filtered_df["kpi_code"] == kpi_code].copy()
    target_kpi = target_kpi.groupby("kpi_facts_date")["kpi_value"].sum().reset_index()
    target_kpi.columns = ["date", "response"]
    # Ensure response is float (not Decimal)
    target_kpi["response"] = pd.to_numeric(target_kpi["response"], errors="coerce")
    
    # Prepare control variables (other KPIs or clusters)
    # Option 1: Use other KPIs as controls
    control_kpis = filtered_df[filtered_df["kpi_code"] != kpi_code].copy()
    
    # Option 2: If use_bu_aggregate=False, use other clusters as controls
    if not use_bu_aggregate:
        # Use other clusters of the same KPI as controls
        same_kpi = filtered_df[filtered_df["kpi_code"] == kpi_code].copy()
        clusters = same_kpi["cluster_label"].unique()
        if len(clusters) > 1:
            # Use first cluster as control (assuming it's unaffected)
            control_cluster = clusters[0]
            control_kpis = same_kpi[same_kpi["cluster_label"] == control_cluster].copy()
            control_kpis = control_kpis.groupby("kpi_facts_date")["kpi_value"].sum().reset_index()
            control_kpis.columns = ["date", "control"]
            # Ensure control is float (not Decimal)
            control_kpis["control"] = pd.to_numeric(control_kpis["control"], errors="coerce")
        else:
            # Fallback: use other KPIs
            control_kpis = filtered_df[filtered_df["kpi_code"] != kpi_code].copy()
            control_kpis = control_kpis.groupby(["kpi_code", "kpi_facts_date"])["kpi_value"].sum().reset_index()
            # Pivot to wide format
            control_kpis = control_kpis.pivot(index="kpi_facts_date", columns="kpi_code", values="kpi_value").reset_index()
            control_kpis.columns.name = None
            control_kpis = control_kpis.rename(columns={"kpi_facts_date": "date"})
            # Sum across control KPIs
            control_cols = [c for c in control_kpis.columns if c != "date"]
            # Convert control columns to float before summing
            for col in control_cols:
                control_kpis[col] = pd.to_numeric(control_kpis[col], errors="coerce")
            control_kpis["control"] = control_kpis[control_cols].sum(axis=1)
            control_kpis = control_kpis[["date", "control"]]
            # Ensure control is float
            control_kpis["control"] = pd.to_numeric(control_kpis["control"], errors="coerce")
    else:
        # For BU aggregate, use other KPIs as controls
        control_kpis = control_kpis.groupby(["kpi_code", "kpi_facts_date"])["kpi_value"].sum().reset_index()
        # Take first few control KPIs (limit to avoid overfitting)
        top_controls = control_kpis["kpi_code"].value_counts().head(3).index
        control_kpis = control_kpis[control_kpis["kpi_code"].isin(top_controls)]
        control_kpis = control_kpis.pivot(index="kpi_facts_date", columns="kpi_code", values="kpi_value").reset_index()
        control_kpis.columns.name = None
        control_kpis = control_kpis.rename(columns={"kpi_facts_date": "date"})
        # Sum across control KPIs
        control_cols = [c for c in control_kpis.columns if c != "date"]
        if len(control_cols) > 0:
            # Convert control columns to float before summing
            for col in control_cols:
                control_kpis[col] = pd.to_numeric(control_kpis[col], errors="coerce")
            control_kpis["control"] = control_kpis[control_cols].sum(axis=1)
            control_kpis = control_kpis[["date", "control"]]
            # Ensure control is float
            control_kpis["control"] = pd.to_numeric(control_kpis["control"], errors="coerce")
        else:
            # No controls available - create dummy
            control_kpis = target_kpi[["date"]].copy()
            control_kpis["control"] = pd.to_numeric(target_kpi["response"], errors="coerce").mean()  # Use mean as control
    
    # Merge response and controls
    data_df = target_kpi.merge(control_kpis, on="date", how="outer")
    data_df = data_df.sort_values("date").reset_index(drop=True)
    
    # Fill missing values (forward fill then backward fill)
    data_df = data_df.ffill().bfill()
    
    # Set date as index
    data_df = data_df.set_index("date")
    
    # Ensure all numeric columns are float (not Decimal)
    data_df["response"] = pd.to_numeric(data_df["response"], errors="coerce")
    if "control" in data_df.columns:
        data_df["control"] = pd.to_numeric(data_df["control"], errors="coerce")
    
    # Ensure we have both response and control columns
    if "control" not in data_df.columns:
        # Create a simple control (moving average of response)
        data_df["control"] = data_df["response"].rolling(window=3, min_periods=1).mean()
    
    # Define periods (CausalImpact expects lists, not tuples)
    pre_period = [pre_start, intervention_date - timedelta(days=1)]
    post_period = [intervention_date, post_end]
    
    _logger.info(
        "Prepared data: %d rows, pre_period=%s to %s, post_period=%s to %s",
        len(data_df),
        pre_period[0].strftime("%Y-%m-%d"),
        pre_period[1].strftime("%Y-%m-%d"),
        post_period[0].strftime("%Y-%m-%d"),
        post_period[1].strftime("%Y-%m-%d"),
    )
    
    return data_df, pre_period, post_period


def analyze_manager_hire_impact(
    manager_code: str,
    kpi_mapping: str,
    geo_codes: list[str],
    organization_level_code: str | None = None,
    kpi_code: str | None = None,
    pre_period_months: int = 12,
    post_period_months: int = 12,
    dal: Dal | None = None,
) -> dict:
    """Analyze the causal impact of a manager hire on MNS KPIs.
    
    This is the main function that orchestrates the analysis:
    1. Gets manager hire dates
    2. Loads MNS KPIs
    3. Prepares data for CausalImpact
    4. Runs CausalImpact analysis
    
    Args:
        manager_code: The manager's hashed employee code.
        kpi_mapping: Domain code (e.g. 'MSLT_GENERAL_MEDICINE').
        geo_codes: Manager's country codes.
        organization_level_code: Optional filter for specific org level.
        kpi_code: Specific KPI to analyze. If None, uses first available.
        pre_period_months: Months before intervention. Default 12.
        post_period_months: Months after intervention. Default 12.
        dal: Optional Dal instance.
        
    Returns:
        Dictionary with:
        - 'hire_dates': DataFrame of hire events
        - 'impact_results': List of CausalImpact results (one per hire event)
        - 'summary': Summary statistics
    """
    # Try to import CausalImpact, handling compatibility issues
    CausalImpact = None
    import_error = None
    
    # Try pycausalimpact first (more compatible)
    try:
        from pycausalimpact import CausalImpact
        _logger.info("Using pycausalimpact library")
    except ImportError:
        try:
            from causalimpact import CausalImpact
            _logger.info("Using causalimpact library")
        except ImportError:
            try:
                from tfcausalimpact import CausalImpact
                _logger.info("Using tfcausalimpact library")
            except ImportError as e:
                import_error = e
            except Exception as e:
                # Handle numpy compatibility issues with tfcausalimpact
                if "get_info" in str(e) or "numpy.__config__" in str(e):
                    import_error = ImportError(
                        f"CausalImpact import failed due to numpy compatibility issue: {e}\n"
                        "Try one of these solutions:\n"
                        "1. Install pycausalimpact: pip install pycausalimpact\n"
                        "2. Downgrade numpy: pip install 'numpy<2.0'\n"
                        "3. Use a different Python environment"
                    )
                else:
                    import_error = e
    
    if CausalImpact is None:
        if import_error:
            raise import_error
        raise ImportError(
            "CausalImpact library not found. Install with:\n"
            "  pip install pycausalimpact  (recommended)\n"
            "  or: pip install tfcausalimpact\n"
            "  or: pip install causalimpact"
        )
    
    if dal is None:
        dal = Dal()
    
    # Step 1: Get manager hire dates
    hire_dates_df = get_manager_hire_dates(
        manager_code, organization_level_code, dal=dal
    )
    
    if len(hire_dates_df) == 0:
        _logger.warning("No hire events found for analysis")
        return {
            "hire_dates": hire_dates_df,
            "impact_results": [],
            "summary": {"error": "No hire events found"},
        }
    
    # Step 2: Load MNS KPIs
    domain_df = load_manager_domain_kpis(
        kpi_mapping=kpi_mapping,
        geo_codes=geo_codes,
        lookback_years=None,  # Get all available history
        dal=dal,
    )
    
    if len(domain_df) == 0:
        _logger.warning("No MNS KPIs found for analysis")
        return {
            "hire_dates": hire_dates_df,
            "impact_results": [],
            "summary": {"error": "No MNS KPIs found"},
        }
    
    # Step 3: Analyze each hire event
    impact_results = []
    
    for idx, hire_row in hire_dates_df.iterrows():
        hire_date = hire_row["snapshot_date"]
        org_level = hire_row.get("organization_level_code", "unknown")
        
        _logger.info(
            "Analyzing hire event %d/%d: date=%s, org_level=%s",
            idx + 1,
            len(hire_dates_df),
            hire_date.strftime("%Y-%m-%d"),
            org_level,
        )
        
        try:
            # Prepare data
            data_df, pre_period, post_period = prepare_causalimpact_data(
                domain_df=domain_df,
                intervention_date=hire_date,
                pre_period_months=pre_period_months,
                post_period_months=post_period_months,
                kpi_code=kpi_code,
                use_bu_aggregate=True,
            )
            
            # Ensure we have enough data
            if len(data_df) < 10:
                _logger.warning("Insufficient data points (%d) for analysis", len(data_df))
                continue
            
            # Run CausalImpact
            # Note: CausalImpact expects pre_period and post_period as lists
            # Convert to lists if they're tuples (for compatibility)
            pre_period_list = list(pre_period) if isinstance(pre_period, tuple) else pre_period
            post_period_list = list(post_period) if isinstance(post_period, tuple) else post_period
            
            # Ensure dates are datetime objects (some libraries expect this)
            # Convert to pandas Timestamp if needed
            pre_period_list = [pd.Timestamp(d) for d in pre_period_list]
            post_period_list = [pd.Timestamp(d) for d in post_period_list]
            
            impact = CausalImpact(
                data=data_df,
                pre_period=pre_period_list,
                post_period=post_period_list,
            )
            
            # Extract summary
            summary = impact.summary()
            
            impact_results.append({
                "hire_date": hire_date,
                "organization_level_code": org_level,
                "kpi_code": kpi_code,  # Store which KPI was analyzed
                "impact": impact,
                "summary": summary,
                "data": data_df,
            })
            
        except Exception as e:
            _logger.error(
                "Error analyzing hire event %s: %s",
                hire_date.strftime("%Y-%m-%d"),
                str(e),
            )
            continue
    
    # Step 4: Create overall summary
    if len(impact_results) > 0:
        overall_summary = {
            "n_hire_events": len(hire_dates_df),
            "n_analyzed": len(impact_results),
            "kpi_code": kpi_code or "auto-selected",
            "kpi_mapping": kpi_mapping,
        }
    else:
        overall_summary = {
            "error": "No successful analyses",
            "n_hire_events": len(hire_dates_df),
        }
    
    return {
        "hire_dates": hire_dates_df,
        "impact_results": impact_results,
        "summary": overall_summary,
    }
