# PPL Manager Analytics

> People KPIs x Business KPIs (M&S, GTM, ...) → cross-domain priorities for managers.
> Agentic loop + MCP tools. Batch monthly + superagent on-demand. Domain-extensible.

This repo is the **data exploration and prototyping layer**. The functions here become MCP tools; the interactive CLI simulates the agent workflow.

---

## Agent Architecture

```
                        ┌──────────────────────────────────────┐
                        │            aily-mcp repo              │
                        │                                      │
                        │  MCP SERVER          SKILL            │
                        │  aily-mcp-ppl        ppl_productivity │
                        │                                      │
                        │  · find_manager      workflow +       │
                        │  · get_profile       analysis rules + │
                        │  · get_manager_kpis  priority format +│
                        │  · get_kpi_drivers   privacy rules    │
                        │  · detect_cross_                      │
                        │    domain_correl.                     │
                        └──────────┬──────────────┬────────────┘
                                   │              │
                                   │  ┌────────┐  │
                                   └──┤  LLM   ├──┘
                                      └───┬────┘
                                          │
                        ┌─────────────────┼─────────────────┐
                        │                                   │
           ┌────────────▼──────────┐       ┌───────────────▼────────────┐
           │  BATCH (monthly)       │       │  SUPERAGENT (on-demand)     │
           │                        │       │                            │
           │  aily-agent-lab:       │       │  Loads same skill +        │
           │  TeamProductivity      │       │  same tools                │
           │  Agent.run()           │       │                            │
           │    → LLM loop          │       │  User question →           │
           │    → PrioritiesOutput  │       │    → LLM loop              │
           │    → BillboardTemplates│       │    → text response         │
           │    → deploy_agent()    │       │                            │
           │    → Frontend          │       │                            │
           └────────────────────────┘       └────────────────────────────┘
```

Same tools, same skill, same LLM. Only the output wrapper differs.

### Execution

**Batch:**
```
For each manager:
  1. Agent.run() → MCPConfig(aily-mcp-ppl + aily-mcp-skills)
  2. LLM loads skill → calls get_manager_kpis → sees flagged KPIs
     → calls get_kpi_drivers → calls detect_cross_domain_correlations
     → synthesizes 1-4 priorities (PrioritiesOutput)
  3. Agent wraps in BillboardTemplates → deploy_agent() → DB → Frontend
```

**Superagent:**
```
User: "Manager abc123 has high attrition. Is it affecting operations?"
  1. Superagent loads ppl_productivity skill via get_skills()
  2. Same LLM loop, same tools
  3. Returns text (not templates)
```

### What goes where

| Component | Repo | Purpose |
|---|---|---|
| MCP tools + SQL + preprocessing | `aily-mcp` (`packages/external/ppl/`) | Data & analysis tools the LLM calls |
| Skill (SKILL.md) | `aily-mcp` (`packages/internal/skills/.../ppl_productivity/`) | Single source of truth for the agent brain |
| Agent class + templates | `aily-agent-lab` (`packages/aily-agent-team-productivity/`) | BaseAgent subclass, BillboardTemplates, deploy_agent() |
| **This repo (ppl)** | `ppl/` | **Data exploration + prototyping** — functions that become MCP tools |

### Tools — what the LLM sees vs what runs inside

The LLM calls tools by name + params. Everything else (SQL, mappings, preprocessing) is internal.

| Tool | LLM calls | What runs inside (hidden from LLM) |
|------|-----------|----------------------------------------------|
| `find_manager` | `(geo, level, location, ...)` | `find_manager.sql` → Dal → candidates |
| `get_manager_profile` | `(manager_code)` | `manager_profile.sql` + `manager_active_teams.sql` → kpi_mapping detection |
| `get_manager_kpis` | `(manager_code, snapshot_date)` | `manager_team_kpis.sql` + `manager_domain_kpis.sql` → Dal → flags |
| `get_kpi_drivers` | `(manager_code, kpi_name)` | drill-down SQL → anonymized breakdown |
| `detect_cross_domain_correlations` | `(manager_code)` | statistical correlation engine |

### Adding a new domain

Adding GTM, Finance, R&D... = SQL + config + mapping. Tool signatures don't change.

```
1. Add sql/gtm/pipeline_kpis.sql
2. Add the BU mapping to config.py (KPI_MAPPING_RULES + KPI_MAPPING_LABELS)
3. Update function_to_domain mappings
4. Done. get_manager_kpis auto-detects the new domain.
```

---

## This repo: data layer + interactive CLI

```
┌─────────────────────────────────────────────────────┐
│                  Interactive CLI                     │  ← You simulating the agent
│                    main.py                           │
└──────────────┬──────────────────────┬───────────────┘
               │                      │
┌──────────────▼──────────┐  ┌────────▼───────────────┐
│   Python functions       │  │   Plotly dashboards    │
│   src/data.py            │  │   src/plots.py         │
│                          │  │                        │
│   find_manager()         │  │   plot_manager_team_   │
│   get_manager_profile()  │  │     dashboard()        │
│   load_manager_team_     │  │   plot_domain_kpi_     │
│     kpis()               │  │     dashboard()        │
│   load_manager_domain_   │  │                        │
│     kpis()               │  │                        │
│   get_available_mns_     │  │                        │
│     clusters()           │  │                        │
│   run_correlation()      │  │                        │
└──────────────┬──────────┘  └────────────────────────┘
               │
┌──────────────▼──────────┐
│   SQL queries            │
│   sql/*.sql              │
└──────────────┬──────────┘
               │
┌──────────────▼──────────┐
│   Aily DAL (Redshift)    │
└─────────────────────────┘
```

### From CLI to MCP tool

Each CLI command maps 1:1 to a future MCP tool:

| CLI command | Python function | Future MCP tool |
|---|---|---|
| `find --geo Germany --level "Exec Level 2"` | `find_manager(geo_code="Germany", management_level="Exec Level 2")` | `find_manager` |
| `select 0` / `profile <hash>` | `get_manager_profile(manager_code)` | `get_manager_profile` |
| `clusters` | `get_available_mns_clusters(business_unit)` | `get_available_clusters` |
| `kpis` | `load_manager_team_kpis()` + `load_manager_domain_kpis()` | `get_manager_kpis` |
| `correlate` | `run_correlation(ppl_data, domain_df)` | `detect_cross_domain_correlations` |
| `explore N` | `plot_correlation_pair(ppl_data, domain_df, mns_kpi, ppl_kpi)` | (visualization — LLM describes chart) |

The functions in `src/data.py` return structured dicts/DataFrames — ready for MCP tool wrapping.

---

## Setup

```bash
# Python 3.11
uv sync

# Environment variables
cp .env.example .env
# Required: AILY_CLOUD_PROVIDER, AILY_TENANT, AILY_ENV, AWS_REGION, AWS_PROFILE
```

---

## Interactive CLI

```bash
uv run python main.py
```

### Workflow

```
find → select N → profile → (clusters) → kpis → correlate → explore N
```

### Commands

**1. Find a manager**

```
> find --geo Germany --level "Exec Level 2" --location Frankfurt
```

Filters: `--geo`, `--level`, `--location`, `--gbu`, `--level-02`, `--function`, `--include-non-managers`

**2. Select a candidate** (auto-loads profile)

```
> 0
```

Or go direct with a known hash:

```
> profile ed19990d01ae19bbb49fe33d6ffd199dfa8f697b0762dda99782bdb1ba2b5cb4
```

**3. View profile** (shows active teams, detected domain, geo_codes)

```
> profile
```

**4. Override domain if auto-detection is wrong**

```
> set-kpi MSLT_VACCINES
```

**5. List available MNS clusters** for the detected BU

```
> clusters
```

**6. Generate dashboards** (opens Plotly in browser)

```
> kpis                  # All BU clusters, active teams only
> kpis 0 3 5            # Only clusters at indices 0, 3, 5
> kpis --all            # Include historical/inactive teams
> kpis --all 0 3 5      # Both flags combined
```

This generates two Plotly dashboards:
- **PPL Team KPIs**: 18 panels (headcount, attrition, health, diversity, risk, etc.) with one line per team
- **Domain KPIs**: BU-specific + Country Organisation KPIs with one line per cluster and dashed targets

After `kpis`, the console suggests running `correlate`.

**7. Root Cause Analysis** (correlation: MNS effect ← PPL cause)

```
> correlate                # Default max_lag=6 months
> correlate --lag 12       # Override to 12 months
```

Uses `aily-ai-correlator` (Granger Causality + Transfer Entropy) to check if changes in PPL KPIs (people) precede changes in MNS KPIs (production). The question it answers: **"production drops — is it people-related?"**

#### Interpreting results

Each row is a (MNS KPI, PPL KPI) pair with significant signal:

| Column | What it is | How to read it |
|---|---|---|
| **MNS KPI** | Business/production KPI (effect) | Grouper: rows below are potential causes |
| **PPL KPI** | People KPI (potential cause) | The indicator that may be driving the MNS change |
| **p** | Granger Causality p-value | Lower = more significant. `< 0.05` = strong |
| **TE** | Transfer Entropy | How much information flows PPL → MNS. Higher = stronger |
| **EE** | Explained Entropy | Fraction of MNS variability explained by PPL (0 to 1) |
| **lag** | Lag in months | Delay between PPL change and MNS impact |

Signal strength indicators:

| Signal | Meaning |
|---|---|
| `*` | Passed Granger test only (TE < 0.1) |
| `**` | Transfer Entropy > 0.1 (moderate relationship) |
| `***` | Transfer Entropy > 0.3 (strong relationship) |

Example output:

```
  MNS KPI: OTIF_RATE
    ***  attrition_rate_pct                 p=0.0012  TE=0.420  EE=0.350  lag=3m
    **   avg_tenure_years                   p=0.0230  TE=0.150  EE=0.120  lag=2m
    *    pct_female                          p=0.0480  TE=0.060  EE=0.040  lag=5m
```

Reading this:
- **Attrition rate** has a **strong** (`***`) link to OTIF. Changes in attrition precede OTIF drops by **3 months**, explaining 35% of its variability.
- **Average tenure** has a **moderate** (`**`) link with 2-month lag.
- **% female** has a **weak** (`*`) signal — likely spurious.

Focus on `***` and `**` pairs. The **lag** tells you how far in advance the people signal predicts the business impact. Results with `p > 0.05` are already filtered out.

**8. Explore a correlation pair** (visual deep dive)

```
> explore 0              # Plot the first correlation pair
> explore 0 2 5          # Plot multiple pairs (opens one chart each)
```

Opens a dual-axis Plotly chart showing both time series superimposed:
- **Left axis** (blue): MNS KPI (effect)
- **Right axis** (orange): PPL KPI (cause)
- The PPL series is shown twice: at its **original position** (faded dotted line) and **shifted forward by lag** (solid line). When the shifted PPL line aligns with the MNS line, the lag relationship becomes visually obvious.
- Correlation metrics (p, TE, EE, lag) are displayed in the chart subtitle.

### How domain KPIs work

Two sources are combined automatically:

| Source | What it fetches | Filter |
|---|---|---|
| **BU KPIs** | All KPIs for the detected Business Unit (e.g. General Medicine) | By default all clusters. Use `clusters` + `kpis <indices>` to narrow down |
| **Country Org KPIs** | Country-level MNS KPIs | Auto-matched to manager's `geo_codes` from PPL profile |

---

## KPI Mapping: how domain detection works

When a manager's profile is loaded, the system auto-detects which MNS domain (Business Unit) is relevant. This mapping connects PPL managers to their MNS KPIs.

### Current approach: heuristic keyword rules

Defined in `src/config.py`:

```python
KPI_MAPPING_RULES = [
    ("general medicines", "MSLT_GENERAL_MEDICINE"),
    ("general medicine",  "MSLT_GENERAL_MEDICINE"),
    ("genmed",            "MSLT_GENERAL_MEDICINE"),
    ("vaccines",          "MSLT_VACCINES"),
    ("vaccine",           "MSLT_VACCINES"),
    ("specialty care",    "MSLT_SPECIALTY_CARE"),
    ("speciality care",   "MSLT_SPECIALTY_CARE"),
]

KPI_MAPPING_LABELS = {
    "MSLT_GENERAL_MEDICINE": "General Medicine",
    "MSLT_VACCINES":         "Vaccines",
    "MSLT_SPECIALTY_CARE":   "Specialty Care",
}
```

The `suggest_kpi_mapping()` function concatenates all profile fields (GBU_Level_1/2/3, Level_02/03/04_From_Top, job_unit) into a single string and scans it for keywords. First match wins.

**To add a new domain:**

1. Add keyword rules to `KPI_MAPPING_RULES` in `src/config.py`
2. Add the human-readable label to `KPI_MAPPING_LABELS`
3. Add the BU → `business_unit_label` mapping to `_KPI_MAPPING_BU` in `src/data.py`
4. The rest (SQL, cluster resolution, dashboards) works automatically

**To modify an existing mapping:**

Edit `KPI_MAPPING_RULES` — rules are evaluated top-to-bottom, first match wins. Put more specific keywords before generic ones.

### Planned: lookup table integration

There is an existing table with pre-computed mappings:

```
supervisory_organization_level_02_from_the_top | business_title | ... | kpi_mapping
Manufacturing and Supply (Brendan O'CALLAGHAN)  | Head of GM...  | ... | MSLT_GENERAL_MEDICINE
Manufacturing and Supply (Brendan O'CALLAGHAN)  | Global Head... | ... | MSLT_VACCINES
```

**TODO**: Replace the heuristic rules with a lookup against this table. The integration plan:

1. Load the mapping table via DAL during `get_manager_profile()`
2. Match on manager attributes (Level_02_From_Top, management_level, geo_code, etc.)
3. Fall back to the current heuristic rules if no table match is found
4. This eliminates ambiguity and supports mappings that keywords can't detect (e.g. managers whose function doesn't mention the BU name)

Until the table is integrated, use `set-kpi <code>` in the CLI (or pass `kpi_mapping` to the function) to override when auto-detection is wrong.

---

## Direct commands (non-interactive)

```bash
# Search for managers
uv run python main.py --find --geo Germany --level "Exec Level 2"

# Generate dashboards for a known manager
uv run python main.py --manager <hash>

# With domain KPIs
uv run python main.py --manager <hash> --kpi-mapping MSLT_GENERAL_MEDICINE
```

---

## Project structure

```
ppl/
├── main.py                     # Entry point (interactive CLI + direct commands)
├── src/
│   ├── __init__.py             # Public API
│   ├── data.py                 # Data loading, aggregation, correlation (future MCP tools)
│   ├── plots.py                # Plotly dashboards (PPL + Domain)
│   ├── config.py               # KPI panel defs, mapping rules, correlatable KPIs, palettes
│   └── paths.py                # Path constants
├── sql/
│   ├── find_manager.sql        # Manager lookup by descriptive filters
│   ├── manager_profile.sql     # Manager self-info + reports context
│   ├── manager_active_teams.sql# Active teams at a snapshot
│   ├── manager_team_kpis.sql   # 21 monthly PPL KPIs per team
│   └── manager_domain_kpis.sql # MNS domain KPIs (BU + Country Org)
├── .env                        # Environment config (not committed)
└── pyproject.toml
```

## PPL KPIs tracked (18 panels)

| Category | Metrics |
|---|---|
| Size & Capacity | Headcount, Attrition Rate 12m |
| Demographics | Average Age, Near Retirement % |
| Tenure & Stability | Avg Tenure, Avg Time in Position |
| Compensation & Diversity | Median Salary, % Female |
| Team Health | Health Score, Development Score, Mobility Score, Succession Score |
| Talent Pipeline | Ready for Promotion %, Succession Candidates % |
| Risk | High Retention Risk %, Critical Flight Risk % |
| Management Structure | Managers in Team %, Avg Span of Control |

## Domain KPIs (MNS)

Dynamically loaded from `mns_kpi_facts` based on detected `kpi_mapping`. Includes all available KPI codes for the Business Unit + Country Organisation KPIs matching the manager's countries.

---

## Possible improvements

- **Correlation sign (Pearson r)**: Currently `correlate` reports Granger p-value, Transfer Entropy, and Explained Entropy — but not whether the relationship is positive or negative. Adding the lagged Pearson correlation coefficient (`r`) would immediately tell users if a PPL KPI increase leads to an MNS KPI increase (`r > 0`) or decrease (`r < 0`), without needing to visually inspect via `explore`.
- **Lookup table for kpi_mapping**: Replace heuristic keyword rules with a database lookup (see "Planned: lookup table integration" above).
- **Additional domains**: Extend beyond M&S to GTM, Finance, R&D — only requires new SQL + config mappings.
