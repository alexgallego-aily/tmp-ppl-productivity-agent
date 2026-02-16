"""PPL Manager Team Analytics — main entry point.

Usage:
    # Interactive console (recommended):
    uv run python main.py

    # Direct commands:
    uv run python main.py --find --geo Germany --level "Exec Level 2" --location "Frankfurt"
    uv run python main.py --manager <hash>
    uv run python main.py --manager <hash> --kpi-mapping MSLT_GENERAL_MEDICINE
"""

import argparse
import logging
import warnings

from dotenv import load_dotenv

load_dotenv(override=True)
warnings.filterwarnings("ignore")

from src import (
    find_manager,
    get_manager_profile,
    get_available_mns_clusters,
    get_manager_summary,
    load_manager_team_kpis,
    load_manager_domain_kpis,
    get_domain_summary,
    resolve_business_unit,
    plot_manager_team_dashboard,
    plot_domain_kpi_dashboard,
    KPI_MAPPING_LABELS,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
_logger = logging.getLogger(__name__)

_B = "\033[1m"   # bold
_D = "\033[0m"   # reset
_G = "\033[90m"  # grey


# =====================================================================
# Interactive console
# =====================================================================

def _ensure_dal(dal):
    """Return a working Dal instance, reconnecting if the connection is stale."""
    from aily_data_access_layer.dal import Dal

    try:
        dal.db._DB__conn.isolation_level  # lightweight liveness check
        return dal
    except Exception:
        _logger.info("DAL connection stale — reconnecting…")
        try:
            dal.db._DB__conn.close()
        except Exception:
            pass
        return Dal()


def _run_interactive():
    """Interactive REPL: find → profile → kpis."""
    from aily_data_access_layer.dal import Dal

    dal = Dal()

    # Session state
    candidates = None    # DataFrame from find
    profile = None        # dict from get_manager_profile
    mns_clusters = None   # list of available clusters for the profile's BU

    _print_banner()

    while True:
        try:
            raw = input(f"\n{_B}>{_D} ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if not raw:
            continue

        parts = raw.split(maxsplit=1)
        cmd = parts[0].lower()
        rest = parts[1] if len(parts) > 1 else ""

        # ── find ──────────────────────────────────────────────────
        if cmd == "find":
            dal = _ensure_dal(dal)
            candidates = _interactive_find(rest, dal)
            profile = None
            mns_clusters = None

        # ── select N  (shortcut after find) ───────────────────────
        elif cmd == "select" or (cmd.isdigit()):
            idx = int(cmd) if cmd.isdigit() else int(rest) if rest.isdigit() else None
            if candidates is None or idx is None:
                print("  Run 'find' first, then 'select <N>' or just type the number.")
                continue
            if idx < 0 or idx >= len(candidates):
                print(f"  Index out of range (0-{len(candidates)-1}).")
                continue
            dal = _ensure_dal(dal)
            manager_code = candidates.iloc[idx]["employee_code"]
            profile = _interactive_profile(manager_code, dal)
            mns_clusters = None

        # ── profile <hash>  (direct) ─────────────────────────────
        elif cmd == "profile":
            mc = rest.strip()
            if not mc:
                if profile:
                    _display_profile(profile, mns_clusters)
                else:
                    print("  Usage: profile <manager_code>")
                continue
            dal = _ensure_dal(dal)
            profile = _interactive_profile(mc, dal)
            mns_clusters = None

        # ── set-kpi  (manual kpi_mapping override) ──────────────
        elif cmd == "set-kpi" or cmd == "setkpi":
            if profile is None:
                print("  Run 'profile' first.")
                continue
            code = rest.strip().upper()
            if not code:
                print("  Usage: set-kpi MSLT_GENERAL_MEDICINE")
                print(f"  Known codes: {', '.join(KPI_MAPPING_LABELS.keys())}")
                continue
            profile["kpi_mapping"] = code
            profile["kpi_mapping_label"] = KPI_MAPPING_LABELS.get(code, code)
            mns_clusters = None
            print(f"  KPI mapping set to: {_B}{code}{_D} ({profile['kpi_mapping_label']})")

        # ── clusters  (show/refine BU clusters) ──────────────────
        elif cmd == "clusters":
            if profile is None or not profile.get("kpi_mapping"):
                print("  Run 'profile' first (need kpi_mapping).")
                continue
            dal = _ensure_dal(dal)
            bu = resolve_business_unit(profile["kpi_mapping"])
            if mns_clusters is None:
                print(f"  Loading available clusters for {bu}…")
                mns_clusters = get_available_mns_clusters(bu, dal=dal)
            print(f"\n  {_B}{bu}{_D} — {len(mns_clusters)} clusters:")
            for i, cl in enumerate(mns_clusters):
                print(f"    [{i:2d}] {cl}")

        # ── kpis  (generate dashboards) ──────────────────────────
        elif cmd == "kpis":
            if profile is None:
                print("  Run 'profile' first.")
                continue
            dal = _ensure_dal(dal)
            show_all_teams = "--all" in rest
            kpi_rest = rest.replace("--all", "").strip()
            selected_bu_clusters = _parse_cluster_selection(kpi_rest, mns_clusters)
            _interactive_kpis(profile, selected_bu_clusters, dal, active_teams_only=not show_all_teams)

        # ── help ──────────────────────────────────────────────────
        elif cmd in ("help", "h", "?"):
            _print_help()

        # ── quit ──────────────────────────────────────────────────
        elif cmd in ("quit", "exit", "q"):
            print("Bye.")
            break

        else:
            print(f"  Unknown command: {cmd}. Type 'help' for options.")


def _print_banner():
    print(f"""
{_B}PPL Manager Analytics — Interactive Console{_D}
{'─' * 50}
Commands: find, select, profile, clusters, kpis, help, quit
""")


def _print_help():
    print(f"""
{_B}Commands:{_D}
  {_B}find{_D} [filters]        Search for managers
                          --level, --geo, --location, --gbu, --level-02, --function
                          --include-non-managers  (default: only is_manager=TRUE)
                          e.g. find --geo Germany --level "Exec Level 2"
                               find --location Frankfurt --include-non-managers

  {_B}select{_D} <N>            Select candidate N from find results
  {_B}<N>{_D}                    Same as 'select N'

  {_B}profile{_D} [<hash>]      Show manager profile (active teams + suggested KPI mapping)
                          Without <hash>: show current profile again.

  {_B}set-kpi{_D} <code>        Override KPI mapping (e.g. set-kpi MSLT_VACCINES)
  {_B}clusters{_D}              List available MNS clusters for the profile's BU

  {_B}kpis{_D} [opts] [idx]     Generate PPL + domain KPI dashboards (Plotly)
                          By default shows only {_B}active teams{_D} (latest month).
                          --all           Include historical/inactive teams too

                          {_B}Domain KPIs:{_D} two sources combined automatically:
                          1. BU KPIs — all clusters for the detected BU
                             (filterable via 'clusters' + indices below)
                          2. Country Org KPIs — auto-matched to manager's
                             geo_codes from PPL profile (e.g. France, Germany)

                          To filter BU clusters:
                            clusters           (list available BU clusters)
                            kpis 0 3 5         (only clusters at those indices)
                          Without indices: all BU clusters

  {_B}help{_D}                  Show this help
  {_B}quit{_D}                  Exit

{_B}Workflow:{_D}  find → select N → profile → (set-kpi / clusters) → kpis
""")


# ── find ──────────────────────────────────────────────────────────

def _interactive_find(filter_str: str, dal) -> "pd.DataFrame | None":
    """Parse filter string and run find_manager."""
    import shlex

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--level", default=None)
    parser.add_argument("--geo", default=None)
    parser.add_argument("--location", default=None)
    parser.add_argument("--gbu", default=None)
    parser.add_argument("--level-02", dest="level_02", default=None)
    parser.add_argument("--function", default=None)
    parser.add_argument("--include-non-managers", dest="include_non_managers",
                        action="store_true", default=False)

    try:
        args = parser.parse_args(shlex.split(filter_str)) if filter_str else parser.parse_args([])
    except SystemExit:
        print("  Bad filters. Example: find --geo Germany --level \"Exec Level 2\"")
        return None

    if not any([args.level, args.geo, args.location, args.gbu, args.level_02, args.function]):
        print("  Provide at least one filter:")
        print("    --level, --geo, --location, --gbu, --level-02, --function")
        return None

    results = find_manager(
        management_level=args.level,
        geo_code=args.geo,
        location_contains=args.location,
        gbu_contains=args.gbu,
        level_02_contains=args.level_02,
        function_contains=args.function,
        is_manager_only=not args.include_non_managers,
        dal=dal,
    )

    if len(results) == 0:
        print("  No candidates found.")
        return None

    print(f"\n  Found {_B}{len(results)}{_D} candidate(s):\n")
    for i, row in results.head(20).iterrows():
        print(
            f"  {_B}[{i}]{_D} {row['employee_code'][:40]}…\n"
            f"      {row.get('employees_managed', '?')} reports | "
            f"{row.get('management_level_code', '')} | "
            f"{row.get('geo_code', '')}\n"
            f"      Location: {row.get('location', '')}\n"
            f"      GBU: {row.get('gbu_level_1', '')} | "
            f"L2: {row.get('level_02_from_top', '')} | "
            f"Func: {row.get('primary_function', '')}"
        )
    if len(results) > 20:
        print(f"\n  … and {len(results) - 20} more. Narrow your filters.")

    print(f"\n  {_G}Type a number to select a candidate (e.g. '0'){_D}")
    return results.reset_index(drop=True)


# ── profile ──────────────────────────────────────────────────────

def _interactive_profile(manager_code: str, dal) -> "dict | None":
    """Load and display manager profile."""
    profile = get_manager_profile(manager_code, dal=dal)
    if profile is None:
        print(f"  Manager {manager_code[:20]}… not found.")
        return None
    _display_profile(profile)
    return profile


def _display_profile(profile: dict, mns_clusters: "list[str] | None" = None):
    """Pretty-print a manager profile."""
    p = profile
    print(f"""
{'═' * 60}
  {_B}Manager Profile{_D}
{'═' * 60}
  Code:        {p['employee_code'][:50]}…
  Level:       {p['management_level']}
  Geo:         {p['geo_code']}
  Location:    {p['location']}
  GBU:         {p['gbu_level_1'] or _G + '(empty)' + _D}
  L2 From Top: {p['level_02_from_top']}
  L3 From Top: {p.get('level_03_from_top', '') or _G + '(empty)' + _D}
  Function:    {p['primary_function'] or _G + '(empty)' + _D}
  {_G}Reports context:{_D}
    GBU L1:    {p.get('reports_gbu_level_1', '') or _G + '(empty)' + _D}
    GBU L2:    {p.get('reports_gbu_level_2', '') or _G + '(empty)' + _D}
    GBU L3:    {p.get('reports_gbu_level_3', '') or _G + '(empty)' + _D}
    L2 Top:    {p.get('reports_level_02', '') or _G + '(empty)' + _D}
    L3 Top:    {p.get('reports_level_03', '') or _G + '(empty)' + _D}
    L4 Top:    {p.get('reports_level_04', '') or _G + '(empty)' + _D}
  Reports:     {p['employees_managed']} ({p.get('direct_report_count', '?')} direct FT regular)
{'─' * 60}
  {_B}Active Teams{_D} (snapshot: {p.get('team_snapshot', '?')}):""")

    if not p["active_teams"]:
        print(f"    {_G}(no teams with org_level resolved — positions may not be synced){_D}")
        if p["geo_codes"]:
            print(f"    Geo codes (from KPIs): {', '.join(p['geo_codes'])}")
    else:
        for t in p["active_teams"]:
            org = t.get("org_level") or "?"
            func = t.get("function", "")
            func_str = f"  [{func}]" if func else ""
            print(f"    {org:<30s}  {t['geo']:<15s}  {t['size']} people{func_str}")
        print(f"  Geo codes: {', '.join(p['geo_codes'])}")

    kpi = p.get("kpi_mapping")
    label = p.get("kpi_mapping_label", "")
    if kpi:
        print(f"""{'─' * 60}
  {_B}Domain KPI Mapping{_D}: {kpi} ({label})
{'═' * 60}""")
    else:
        print(f"""{'─' * 60}
  {_B}Domain KPI Mapping{_D}: {_G}(not detected — set with: set-kpi MSLT_GENERAL_MEDICINE){_D}
{'═' * 60}""")

    if mns_clusters:
        print(f"  Available BU clusters: {len(mns_clusters)}")

    print(f"\n  {_G}Type 'clusters' to see BU clusters, 'kpis' to generate dashboards{_D}")


# ── kpis ──────────────────────────────────────────────────────────

def _parse_cluster_selection(
    rest: str,
    mns_clusters: "list[str] | None",
) -> "list[str] | None":
    """Parse cluster indices from the kpis command."""
    if not rest.strip() or mns_clusters is None:
        return None
    try:
        indices = [int(x) for x in rest.split()]
        selected = [mns_clusters[i] for i in indices if 0 <= i < len(mns_clusters)]
        return selected if selected else None
    except (ValueError, IndexError):
        return None


def _interactive_kpis(
    profile: dict,
    bu_clusters: "list[str] | None",
    dal,
    active_teams_only: bool = True,
):
    """Generate PPL + domain dashboards for the current profile."""
    manager_code = profile["employee_code"]
    geo_codes = profile["geo_codes"]
    kpi_mapping = profile.get("kpi_mapping")

    # ── PPL KPIs ─────────────────────────────────────────────────
    print(f"\n  {_B}[1/2] PPL Team KPIs{_D}")
    data = load_manager_team_kpis(manager_code, dal=dal)

    if len(data) == 0:
        print("  No PPL data found.")
        return

    # Filter to active teams (present in the latest month) by default
    all_teams_count = data.groupby(["organization_level_code", "geo_code"]).ngroups
    if active_teams_only and len(data) > 0:
        latest_month = data["month"].max()
        active = data[data["month"] == latest_month][
            ["organization_level_code", "geo_code"]
        ].drop_duplicates()
        data = data.merge(active, on=["organization_level_code", "geo_code"], how="inner")
        active_count = data.groupby(["organization_level_code", "geo_code"]).ngroups
        if active_count < all_teams_count:
            print(
                f"  Showing {_B}{active_count}{_D} active teams "
                f"(of {all_teams_count} total). Use 'kpis --all' to see all."
            )

    summary = get_manager_summary(data)
    print(
        f"  Teams: {summary['n_teams']} | "
        f"Geos: {', '.join(summary['geos'])} | "
        f"HC: {summary['total_headcount']} | "
        f"Period: {summary['months_range'][0]} → {summary['months_range'][1]}"
    )

    fig = plot_manager_team_dashboard(data, manager_code)
    if fig is not None:
        fig.show()

    # ── Domain KPIs ──────────────────────────────────────────────
    if not kpi_mapping:
        print(f"\n  {_G}No kpi_mapping detected. Skipping domain KPIs.{_D}")
        print(f"  {_G}To set manually, update profile['kpi_mapping'] or use direct mode.{_D}")
        return

    bu = resolve_business_unit(kpi_mapping)
    cluster_desc = f"{len(bu_clusters)} selected" if bu_clusters else "all"
    print(f"\n  {_B}[2/2] Domain KPIs: {bu}{_D} ({cluster_desc} clusters)")

    domain_df = load_manager_domain_kpis(
        kpi_mapping, geo_codes, bu_clusters=bu_clusters, dal=dal,
    )

    if len(domain_df) == 0:
        print("  No domain KPIs found.")
        return

    dsummary = get_domain_summary(domain_df)
    print(
        f"  BU clusters: {len(dsummary['domain_clusters'])} | "
        f"BU KPIs: {len(dsummary['domain_kpi_codes'])} | "
        f"Country matches: {', '.join(dsummary['country_geos']) or 'none'} | "
        f"Dates: {dsummary['date_range'][0]} → {dsummary['date_range'][1]}"
    )

    domain_fig = plot_domain_kpi_dashboard(
        domain_df,
        manager_code,
        business_unit=bu,
        kpi_mapping_label=profile.get("kpi_mapping_label", ""),
    )
    if domain_fig is not None:
        domain_fig.show()
    else:
        _print_domain_kpi_sample(domain_df)


def _print_domain_kpi_sample(domain_df):
    """Print a compact sample of available domain KPIs."""
    import pandas as pd
    for source_label, source_key in [("BU KPIs", "domain"), ("Country KPIs", "country_org")]:
        subset = domain_df[domain_df["source"] == source_key]
        if len(subset) == 0:
            continue

        print(f"\n  {source_label}:")
        kpi_summary = (
            subset.groupby("kpi_code")
            .agg(
                clusters=("cluster_label", "nunique"),
                date_points=("kpi_facts_date", "nunique"),
                latest_value=("kpi_value", "last"),
            )
            .sort_values("date_points", ascending=False)
        )
        for kpi_code, row in kpi_summary.head(15).iterrows():
            val_str = f"{row['latest_value']:.2f}" if pd.notna(row["latest_value"]) else "N/A"
            print(
                f"    {kpi_code:<40s}  "
                f"{int(row['clusters'])} clusters  "
                f"{int(row['date_points'])} dates  "
                f"latest={val_str}"
            )
        remaining = len(kpi_summary) - 15
        if remaining > 0:
            print(f"    … and {remaining} more KPI codes")


# =====================================================================
# Direct commands (non-interactive)
# =====================================================================

def _run_find(args):
    """Search for manager candidates and print results."""
    from aily_data_access_layer.dal import Dal

    dal = Dal()
    results = find_manager(
        management_level=args.level,
        geo_code=args.geo,
        location_contains=args.location,
        gbu_contains=args.gbu,
        level_02_contains=args.level_02,
        function_contains=args.function,
        is_manager_only=not args.include_non_managers,
        dal=dal,
    )

    if len(results) == 0:
        print("No candidates found with the given filters.")
        return

    print(f"\n{'─' * 80}")
    print(f"Found {len(results)} candidate(s):")
    print(f"{'─' * 80}\n")

    for i, row in results.head(20).iterrows():
        print(
            f"  [{i}] {row['employee_code'][:40]}…\n"
            f"      Manager of: {row.get('employees_managed', '?')} employees | "
            f"{row.get('management_level_code', '')} | "
            f"{row.get('geo_code', '')}\n"
            f"      Location: {row.get('location', '')}\n"
            f"      GBU: {row.get('gbu_level_1', '')} | "
            f"L2: {row.get('level_02_from_top', '')} | "
            f"Func: {row.get('primary_function', '')}\n"
        )

    if len(results) > 20:
        print(f"  … and {len(results) - 20} more. Narrow your filters.\n")

    print(f"{'─' * 80}")
    top = results.iloc[0]["employee_code"]
    print(f"  uv run python main.py --manager {top}")
    print(f"{'─' * 80}")


def _run_manager(manager_code: str, geo_filter=None, kpi_mapping=None):
    """Direct mode: load KPIs for one manager."""
    from aily_data_access_layer.dal import Dal

    dal = Dal()

    data = load_manager_team_kpis(manager_code, dal=dal)
    if len(data) == 0:
        _logger.error("No data found for manager %s", manager_code)
        return

    if geo_filter:
        data = data[data["geo_code"] == geo_filter].reset_index(drop=True)
        if len(data) == 0:
            _logger.error("No data for geo_code=%s", geo_filter)
            return

    summary = get_manager_summary(data)
    print(
        f"\n{'═' * 70}"
        f"\n  Manager: {manager_code[:40]}…"
        f"\n  Teams: {summary['n_teams']} | "
        f"Geos: {', '.join(summary['geos'])} | "
        f"Functions: {', '.join(summary['functions'][:3])}"
        f"\n  HC: {summary['total_headcount']} | "
        f"FTE: {summary['total_fte']} | "
        f"Period: {summary['months_range'][0]} → {summary['months_range'][1]}"
        f"\n{'═' * 70}"
    )

    fig = plot_manager_team_dashboard(data, manager_code)
    if fig is not None:
        fig.show()

    if kpi_mapping:
        geo_codes = summary["geos"]
        domain_df = load_manager_domain_kpis(kpi_mapping, geo_codes, dal=dal)
        if len(domain_df) > 0:
            dsummary = get_domain_summary(domain_df)
            bu = dsummary.get("business_unit", kpi_mapping)
            print(
                f"\n  Domain KPIs: {bu} | "
                f"{len(dsummary['domain_clusters'])} clusters | "
                f"{len(dsummary['domain_kpi_codes'])} KPIs | "
                f"Countries: {', '.join(dsummary['country_geos'])}"
            )
            _print_domain_kpi_sample(domain_df)


# =====================================================================
# CLI
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="PPL Manager Team Analytics")

    parser.add_argument("--manager", type=str, default=None,
                        help="Manager code hash (direct mode).")
    parser.add_argument("--geo", type=str, default=None,
                        help="Filter by geo_code (e.g. France).")
    parser.add_argument("--kpi-mapping", type=str, default=None, dest="kpi_mapping",
                        help="Domain KPI mapping code (e.g. MSLT_GENERAL_MEDICINE).")

    parser.add_argument("--find", action="store_true",
                        help="Search for managers (direct mode).")
    parser.add_argument("--level", type=str, default=None)
    parser.add_argument("--location", type=str, default=None)
    parser.add_argument("--gbu", type=str, default=None)
    parser.add_argument("--level-02", type=str, default=None, dest="level_02")
    parser.add_argument("--function", type=str, default=None)
    parser.add_argument("--include-non-managers", action="store_true", default=False)

    args = parser.parse_args()

    if args.find:
        _run_find(args)
    elif args.manager:
        _run_manager(args.manager, args.geo, args.kpi_mapping)
    else:
        _run_interactive()


if __name__ == "__main__":
    main()
