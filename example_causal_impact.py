"""Example: Using CausalImpact to analyze manager hire impact on MNS KPIs.

This script demonstrates how to use the causal_impact module to analyze
whether hiring a manager affects MNS (M&S) KPI time series.

Usage:
    uv run python example_causal_impact.py --manager <manager_code> --kpi-mapping MSLT_GENERAL_MEDICINE
"""

import argparse
import logging
from dotenv import load_dotenv

load_dotenv(override=True)

from aily_data_access_layer.dal import Dal
from src.causal_impact import analyze_manager_hire_impact
from src.data import get_manager_profile

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
_logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze manager hire impact on MNS KPIs using CausalImpact"
    )
    parser.add_argument(
        "--manager",
        required=True,
        help="Manager's hashed employee code",
    )
    parser.add_argument(
        "--kpi-mapping",
        required=True,
        help="KPI mapping code (e.g., MSLT_GENERAL_MEDICINE)",
    )
    parser.add_argument(
        "--org-level",
        default=None,
        help="Optional: Filter by organization_level_code",
    )
    parser.add_argument(
        "--kpi-code",
        default=None,
        help="Optional: Specific KPI code to analyze (default: first available)",
    )
    parser.add_argument(
        "--pre-months",
        type=int,
        default=12,
        help="Months before intervention for training (default: 12)",
    )
    parser.add_argument(
        "--post-months",
        type=int,
        default=12,
        help="Months after intervention for analysis (default: 12)",
    )
    
    args = parser.parse_args()
    
    dal = Dal()
    
    # Get manager profile to extract geo_codes
    profile = get_manager_profile(args.manager, dal=dal)
    if profile is None:
        _logger.error("Manager %s not found", args.manager)
        return
    
    geo_codes = profile.get("geo_codes", [])
    if not geo_codes:
        _logger.warning("No geo_codes found for manager, using empty list")
        geo_codes = []
    
    _logger.info("Analyzing manager hire impact...")
    _logger.info("Manager: %s", args.manager[:16])
    _logger.info("KPI Mapping: %s", args.kpi_mapping)
    _logger.info("Geo Codes: %s", geo_codes)
    
    # Run the analysis
    results = analyze_manager_hire_impact(
        manager_code=args.manager,
        kpi_mapping=args.kpi_mapping,
        geo_codes=geo_codes,
        organization_level_code=args.org_level,
        kpi_code=args.kpi_code,
        pre_period_months=args.pre_months,
        post_period_months=args.post_months,
        dal=dal,
    )
    
    # Display results
    print("\n" + "=" * 80)
    print("CAUSAL IMPACT ANALYSIS RESULTS")
    print("=" * 80)
    
    print(f"\nHire Events Found: {len(results['hire_dates'])}")
    print(f"Successfully Analyzed: {len(results['impact_results'])}")
    
    if len(results['impact_results']) == 0:
        print("\nNo successful analyses. Check logs for details.")
        return
    
    # Display overall summary
    if 'summary' in results and 'kpi_code' in results['summary']:
        print(f"\nKPI Analyzed: {results['summary']['kpi_code']}")
        if results['summary']['kpi_code'] == 'auto-selected':
            print("  (Auto-selected: first available KPI)")
    
    # Display summary for each analysis
    for i, result in enumerate(results['impact_results'], 1):
        print(f"\n{'─' * 80}")
        print(f"Analysis {i}/{len(results['impact_results'])}")
        print(f"Hire Date: {result['hire_date'].strftime('%Y-%m-%d')}")
        print(f"Organization Level: {result['organization_level_code']}")
        if 'kpi_code' in result:
            print(f"KPI Code: {result['kpi_code']}")
        print(f"\nCausalImpact Summary:")
        print(result['summary'])
        print(f"\n{'─' * 80}")
    
    # Example: Plot the first result
    if len(results['impact_results']) > 0:
        try:
            impact = results['impact_results'][0]['impact']
            print("\nGenerating plot for first analysis...")
            impact.plot()
            
            # Note: If using tfcausalimpact, you might need:
            # import matplotlib.pyplot as plt
            # plt.show()
        except Exception as e:
            _logger.warning("Could not generate plot: %s", str(e))
            print("\nNote: Install matplotlib to view plots, or use impact.summary() for text output")


if __name__ == "__main__":
    main()
