"""PPL Manager Team Analytics — main entry point.

Usage:
    uv run python main.py                      # global dashboard
    uv run python main.py --manager <hash>     # single manager dashboard
    uv run python main.py --demo               # pre-selected demo managers
"""

import argparse
import logging
import warnings

warnings.filterwarnings("ignore")

from src import (
    get_latest,
    load_all,
    plot_global_dashboard,
    plot_manager_dashboard,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
_logger = logging.getLogger(__name__)


def _print_summary(df, mns, merged):
    """Print a one-line data summary after loading."""
    latest = get_latest(df)
    bu_mapped = df["mns_business_unit"].notna().sum()
    cl_mapped = df["mns_cluster"].notna().sum()
    print(
        f"Loaded: {df.shape[0]:,} rows | "
        f"{df['manager_code'].nunique():,} managers | "
        f"{df['geo_code'].nunique()} countries | "
        f"{df['month'].min().strftime('%Y-%m')} to {df['month'].max().strftime('%Y-%m')}"
    )
    print(
        f"M&S KPIs: {len(mns):,} rows | "
        f"Merged: {merged['headcount'].notna().sum():,} rows with People data"
    )
    print(
        f"BU mapped: {bu_mapped:,} ({bu_mapped / len(df) * 100:.1f}%) | "
        f"Cluster mapped: {cl_mapped:,} ({cl_mapped / len(df) * 100:.1f}%)"
    )


def _pick_demo_managers(df):
    """Return a list of (manager_code, description) for demo dashboards."""
    demos = []

    # 1. Swiftwater Vaccines manager (site-level M&S overlay)
    sw = df[
        (df["mns_cluster"] == "Swiftwater (Cluster)")
        & (df["mns_business_unit"] == "Vaccines")
    ]
    if len(sw) > 0:
        cands = (
            sw.groupby("manager_code")
            .agg(months=("month", "nunique"), hc=("headcount", "mean"))
            .query("months >= 24 and hc >= 8")
            .sort_values("hc", ascending=False)
        )
        if len(cands) > 0:
            demos.append((cands.index[0], "Swiftwater Vaccines (site-level M&S)"))

    # 2. BU-level M&S manager (Anagni, General Medicine)
    bu_manager = "65bf66352d7819587ce372d7a1a25a0ecfd98fa095cbb3662d5a65452e0736b9"
    if bu_manager in df["manager_code"].values:
        demos.append((bu_manager, "Anagni General Medicine (BU-level M&S)"))

    return demos


def main():
    parser = argparse.ArgumentParser(description="PPL Manager Team Analytics")
    parser.add_argument(
        "--manager",
        type=str,
        default=None,
        help="Manager code hash to display a single manager dashboard.",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Show pre-selected demo manager dashboards.",
    )
    parser.add_argument(
        "--no-global",
        action="store_true",
        help="Skip the global overview dashboard.",
    )
    args = parser.parse_args()

    # ── Load data ────────────────────────────────────────────────────
    _logger.info("Loading data …")
    df, mns, merged = load_all()
    _print_summary(df, mns, merged)

    # ── Global dashboard ─────────────────────────────────────────────
    if not args.no_global and not args.manager:
        _logger.info("Generating global dashboard …")
        plot_global_dashboard(df).show()

    # ── Single manager ───────────────────────────────────────────────
    if args.manager:
        _show_manager(df, mns, args.manager)

    # ── Demo managers ────────────────────────────────────────────────
    if args.demo:
        demos = _pick_demo_managers(df)
        if not demos:
            _logger.warning("No demo managers found in the dataset.")
        for mgr_code, description in demos:
            _logger.info(f"Demo: {description}")
            _show_manager(df, mns, mgr_code)


def _show_manager(df, mns, manager_code):
    """Print manager context and show dashboard."""
    mgr_rows = df[df["manager_code"] == manager_code]
    if len(mgr_rows) == 0:
        _logger.error(f"Manager {manager_code} not found in the dataset.")
        return

    mgr = mgr_rows.iloc[-1]
    print(
        f"\nManager: {manager_code[:40]}…\n"
        f"  Country: {mgr['geo_code']} | "
        f"Cluster: {mgr.get('location_cluster', 'N/A')} | "
        f"Function: {mgr['primary_function']} | "
        f"GBU: {mgr.get('primary_gbu_level_1', '')}\n"
        f"  Team: {mgr['headcount']:.0f} employees | "
        f"Health: {mgr['team_health_score']:.1f} | "
        f"Attrition: {mgr['attrition_rate_12m_pct']:.1f}%"
    )

    fig = plot_manager_dashboard(df, manager_code, mns=mns)
    if fig is not None:
        fig.show()


if __name__ == "__main__":
    main()
