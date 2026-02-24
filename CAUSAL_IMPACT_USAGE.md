# How to Run CausalImpact Analysis

This guide shows you how to analyze the impact of manager hires on MNS KPIs using CausalImpact.

## Prerequisites

### 1. Install CausalImpact Library

You need to install one of the Python CausalImpact libraries. The code tries multiple options automatically.

**Option A: pycausalimpact (recommended - most compatible)**
```bash
uv pip install pycausalimpact
```

**Option B: tfcausalimpact (may have numpy compatibility issues)**
```bash
# If you get numpy compatibility errors, downgrade numpy first:
uv pip install 'numpy<2.0'
uv pip install tfcausalimpact
```

**Option C: causalimpact**
```bash
uv pip install causalimpact
```

**Note:** If you encounter `AttributeError: module 'numpy.__config__' has no attribute 'get_info'`, this is a known compatibility issue between `tfcausalimpact` and numpy 2.0+. Use `pycausalimpact` instead or downgrade numpy to < 2.0.

## Step-by-Step Usage

### Step 1: Find a Manager Code

First, you need a manager's employee code. Use the existing CLI:

```bash
# Find managers by criteria
uv run python main.py --find --geo Germany --level "Exec Level 2"

# Or use the interactive console
uv run python main.py
> find --geo Germany --level "Exec Level 2"
> select 0  # Select the first result
```

The manager code will be displayed (it's a hash like `abc123def456...`).

### Step 2: Run the CausalImpact Analysis

#### Option A: Using the Example Script (Recommended)

```bash
uv run python example_causal_impact.py \
  --manager <manager_code> \
  --kpi-mapping MSLT_GENERAL_MEDICINE
```

**Full example with all options:**
```bash
uv run python example_causal_impact.py \
  --manager abc123def456... \
  --kpi-mapping MSLT_GENERAL_MEDICINE \
  --org-level "ORG_LEVEL_123" \
  --kpi-code "SALES_VOLUME" \
  --pre-months 12 \
  --post-months 12
```

**Parameters:**
- `--manager` (required): Manager's hashed employee code
- `--kpi-mapping` (required): Domain code (e.g., `MSLT_GENERAL_MEDICINE`, `MSLT_VACCINES`, `MSLT_SPECIALTY_CARE`)
- `--org-level` (optional): Filter by specific organization_level_code
- `--kpi-code` (optional): Specific KPI code to analyze (default: first available)
- `--pre-months` (optional): Months before intervention for training (default: 12)
- `--post-months` (optional): Months after intervention for analysis (default: 12)

#### Option B: Using Python Code Directly

```python
from aily_data_access_layer.dal import Dal
from src.causal_impact import analyze_manager_hire_impact
from src.data import get_manager_profile

# Initialize DAL
dal = Dal()

# Get manager profile to extract geo_codes
manager_code = "abc123def456..."  # Your manager code
profile = get_manager_profile(manager_code, dal=dal)
geo_codes = profile.get("geo_codes", [])

# Run the analysis
results = analyze_manager_hire_impact(
    manager_code=manager_code,
    kpi_mapping="MSLT_GENERAL_MEDICINE",
    geo_codes=geo_codes,
    organization_level_code=None,  # Optional filter
    kpi_code=None,  # Auto-selects first available
    pre_period_months=12,
    post_period_months=12,
    dal=dal,
)

# Access results
print(f"Found {len(results['hire_dates'])} hire events")
print(f"Successfully analyzed {len(results['impact_results'])} events")

# For each analysis result
for result in results['impact_results']:
    print(f"\nHire Date: {result['hire_date']}")
    print(f"Summary:\n{result['summary']}")
    
    # Plot the impact (requires matplotlib)
    # result['impact'].plot()
```

## Understanding the Output

The analysis returns a dictionary with:

1. **`hire_dates`**: DataFrame of all hire events found
2. **`impact_results`**: List of analysis results, each containing:
   - `hire_date`: When the manager was hired
   - `organization_level_code`: The organization level
   - `impact`: CausalImpact object (use `.plot()` and `.summary()`)
   - `summary`: Text summary of the impact
   - `data`: Prepared time series data
3. **`summary`**: Overall summary statistics

### Interpreting Results

The CausalImpact summary shows:
- **Average**: Average effect during post-intervention period
- **Cumulative**: Total cumulative effect
- **P-value**: Statistical significance
- **Posterior tail-area probability**: Probability of observing this effect

A significant positive effect means the manager hire **improved** the KPI.
A significant negative effect means the manager hire **decreased** the KPI.

## Troubleshooting

### "CausalImpact library not found" or numpy compatibility errors

**If you see `AttributeError: module 'numpy.__config__' has no attribute 'get_info'`:**

This is a compatibility issue between `tfcausalimpact` and numpy 2.0+. Try:

**Solution 1 (Recommended):** Use `pycausalimpact` instead:
```bash
uv pip install pycausalimpact
```

**Solution 2:** Downgrade numpy:
```bash
uv pip install 'numpy<2.0'
uv pip install tfcausalimpact
```

**Solution 3:** Install in a fresh environment:
```bash
python -m venv .venv-causalimpact
source .venv-causalimpact/bin/activate  # On Windows: .venv-causalimpact\Scripts\activate
pip install pycausalimpact
```

### "No hire events found"
- Check that the manager code is correct
- Verify the manager has employees who were hired (check `ppl_moves` table)
- Try removing the `--org-level` filter

### "No MNS KPIs found"
- Verify the `kpi_mapping` matches the manager's business unit
- Check that the manager's `geo_codes` have corresponding MNS data
- Try using `get_available_mns_clusters()` to see what's available

### "Insufficient data points"
- Increase `--pre-months` to get more training data
- Check that MNS KPIs exist for the date range
- Verify the intervention date is within the KPI date range

## Example Workflow

```bash
# 1. Find a manager
uv run python main.py --find --geo Germany

# 2. Copy the manager code from the output

# 3. Run CausalImpact analysis
uv run python example_causal_impact.py \
  --manager <copied_manager_code> \
  --kpi-mapping MSLT_GENERAL_MEDICINE

# 4. Review the results and check for significant impacts
```

## Next Steps

- Visualize results: Use `result['impact'].plot()` to see the counterfactual prediction
- Export data: Access `result['data']` to get the prepared time series
- Customize analysis: Modify `prepare_causalimpact_data()` to use different control variables
