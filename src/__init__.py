"""
src — Manager Productivity Analytics.

Data is loaded from the database via Aily's Data Access Layer (DAL).
SQL queries live in the ``sql/`` directory at the project root.

Usage:
    from src import load_all, plot_global_dashboard, plot_manager_dashboard
"""

from .data import (
    get_latest,
    get_manager_mns_kpis,
    load_all,
    load_data,
    load_mns_kpis,
)
from .plots import plot_global_dashboard, plot_manager_dashboard
from .config import BENCHMARK_METRICS
from .mapping import enrich_people, enrich_mns, merge_mns_people
