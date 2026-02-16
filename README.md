# PPL Manager Analytics

Per-manager People KPIs + MNS domain KPIs. Interactive CLI for data exploration and prototyping the AI agent workflow.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Interactive CLI                     │  ← You (or QA) exploring data
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
└──────────────┬──────────┘  └────────────────────────┘
               │
┌──────────────▼──────────┐
│   SQL queries            │
│   sql/*.sql              │
│                          │
│   find_manager.sql       │
│   manager_profile.sql    │
│   manager_active_teams   │
│   manager_team_kpis.sql  │
│   manager_domain_kpis    │
└──────────────┬──────────┘
               │
┌──────────────▼──────────┐
│   Aily DAL (Redshift)    │
└─────────────────────────┘
```

### From CLI to Agent

The interactive CLI simulates what an AI agent would do. Each CLI command maps 1:1 to a future MCP tool:

| CLI command | Python function | Future MCP tool |
|---|---|---|
| `find --geo Germany --level "Exec Level 2"` | `find_manager(geo_code="Germany", management_level="Exec Level 2")` | `find_manager` |
| `select 0` / `profile <hash>` | `get_manager_profile(manager_code)` | `get_manager_profile` |
| `clusters` | `get_available_mns_clusters(business_unit)` | `get_available_clusters` |
| `kpis` | `load_manager_team_kpis()` + `load_manager_domain_kpis()` | `get_manager_kpis` |

The functions in `src/data.py` return structured dicts/DataFrames — ready for MCP tool wrapping.

---

## Setup

```bash
# Python 3.11
uv sync

# Environment variables (copy and fill)
cp .env.example .env
# Required: AILY_CLOUD_PROVIDER, AILY_TENANT, AILY_ENV, AWS_REGION, AWS_PROFILE
```

---

## Interactive CLI

The recommended way to explore. Start with:

```bash
uv run python main.py
```

### Workflow

```
find → select N → profile → (clusters) → kpis
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

### How domain KPIs work

Two sources are combined automatically:

| Source | What it fetches | Filter |
|---|---|---|
| **BU KPIs** | All KPIs for the detected Business Unit (e.g. General Medicine) | By default all clusters. Use `clusters` + `kpis <indices>` to narrow down |
| **Country Org KPIs** | Country-level MNS KPIs | Auto-matched to manager's `geo_codes` from PPL profile |

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
│   ├── data.py                 # Data loading functions (tool-ready)
│   ├── plots.py                # Plotly dashboards
│   ├── config.py               # KPI definitions, mapping rules, palettes
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

## KPIs tracked

### PPL Team KPIs (18 panels)

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

### Domain KPIs (MNS)

Dynamically loaded from `mns_kpi_facts` based on detected `kpi_mapping`. Includes all available KPI codes for the Business Unit + Country Organisation KPIs matching the manager's countries.
