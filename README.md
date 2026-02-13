# Productivity Agent — Architecture

> People KPIs × Business KPIs (M&S, GTM, ...) → cross-domain priorities for managers.
> Agentic loop + MCP tools. Batch monthly + superagent on-demand. Domain-extensible.

---

## How it works

```
                        ┌──────────────────────────────────────┐
                        │            aily-mcp repo              │
                        │                                      │
                        │  MCP SERVER          SKILL            │
                        │  aily-mcp-ppl        ppl_productivity │
                        │                                      │
                        │  · get_manager_kpis  workflow +       │
                        │  · get_kpi_drivers   analysis rules + │
                        │  · detect_cross_     priority format +│
                        │    domain_correl.    privacy rules    │
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

---

## Execution

### Batch

```
For each manager:
  1. Agent.run() → MCPConfig(aily-mcp-ppl + aily-mcp-skills)
  2. LLM loads skill → calls get_manager_kpis → sees flagged KPIs
     → calls get_kpi_drivers → calls detect_cross_domain_correlations
     → synthesizes 1-4 priorities (PrioritiesOutput)
  3. Agent wraps in BillboardTemplates → deploy_agent() → DB → Frontend
```

```bash
uv run aily-agent-team-productivity team-productivity-agent-publish \
    --snapshot-date 2024-12-01 --parallel --workers 5
```

### Superagent

```
User: "Manager abc123 has high attrition. Is it affecting operations?"
  1. Superagent loads ppl_productivity skill via get_skills()
  2. Same LLM loop, same tools
  3. Returns text (not templates)
```

---

## What goes where

### `aily-mcp` — MCP server + skill

```
packages/external/ppl/
├── src/aily_mcp_ppl/
│   ├── main.py                      # Server entry
│   ├── tools/
│   │   ├── kpis.py                  #   get_manager_kpis
│   │   ├── drivers.py               #   get_kpi_drivers
│   │   └── correlations.py          #   detect_cross_domain_correlations
│   ├── sql/
│   │   ├── ppl/                     #   People KPI queries
│   │   ├── mns/                     #   M&S queries
│   │   └── .../                     #   ... other domain queries
│   ├── preprocessing/
│   │   ├── percentiles.py           #   Percentile computation
│   │   └── anomaly_flags.py         #   Flag outliers + trends
│   ├── mappings/
│   │   ├── domain_resolver.py       #   function → domain (Production→M&S, Sales→GTM)
│   │   ├── mns_mapping.py           #   BU + cluster resolution
│   │   └── .../                     #   ... other domain mappings
│   ├── connections/
│   │   ├── correlations.py          #   Statistical correlation engine
│   │   └── .../                     #   ... root causes, causal impact (future)
│   └── configs/
│       ├── sanofi.yaml              #   Tenant config
│       ├── domains/
│       │   ├── ppl.yaml             #   People KPI definitions
│       │   ├── mns.yaml             #   M&S KPI definitions
│       │   └── .../                 #   ... other domain configs
│       └── mappings/
│           └── function_to_domain.yaml

packages/internal/skills/.../ppl_productivity/
└── SKILL.md                         # Single source of truth for the agent brain
```

### `aily-agent-lab` — Agent class + templates

```
packages/aily-agent-team-productivity/
├── aily_agent_team_productivity/
│   ├── cli.py                       # Click CLI (exists)
│   ├── runners.py                   # deploy_agent() wrapper (exists)
│   ├── agent/
│   │   ├── team_productivity_agent.py   # BaseAgent subclass
│   │   ├── schemas.py                   # PrioritiesOutput
│   │   ├── templates/
│   │   │   ├── billboard_leaderboard.py
│   │   │   └── billboard_detail.py
│   │   └── config/
│   │       └── config.yaml              # servers, LLM, parallel settings
├── tests/
│   └── test_imports.py
└── pyproject.toml
```

### How they connect

```python
# Inside TeamProductivityAgent.run():

config = MCPConfig(
    servers={
        "aily-mcp-ppl": "==1.0.0",        # data + analysis tools
        "aily-mcp-skills": "==1.4.5",      # skill loader → no duplication
    },
    system_prompt="Load ppl_productivity skill. Analyze manager {id}. Return PrioritiesOutput.",
    llm=BedrockModelID.LatestSonnet,
)
priorities = asyncio.run(run_question(config, question, output_type=PrioritiesOutput))
# → wrap in BillboardTemplates → return (templates, structure, metadata)
```

---

## Tools — what the LLM sees vs what runs inside

The LLM calls tools by name + params. Everything else (SQL, mappings, preprocessing)
is **internal to the tool** — the LLM never sees it. Same pattern as `aily-mcp-fin`.

| Tool | LLM calls | What runs inside the tool (hidden from LLM) |
|------|-----------|----------------------------------------------|
| `get_manager_kpis` | `(manager_code, snapshot_date)` | domain_resolver → sql/{domain}/ → Dal → percentiles → anomaly_flags → JSON |
| `get_kpi_drivers` | `(manager_code, kpi_name, snapshot_date)` | sql/ppl/driver_detail.sql → Dal → anonymized drill-down → JSON |
| `detect_cross_domain_correlations` | `(manager_code, snapshot_date)` | connections/correlations.py → statistical engine → JSON |

### Inside `get_manager_kpis` (example flow)

```
LLM calls:  get_manager_kpis(manager_code="abc123", snapshot_date="2024-12-01")
                │
                │   ┌─── INTERNAL (tool code, invisible to LLM) ──────┐
                │   │                                                  │
                ▼   │  1. configs/domains/*.yaml → load KPI defs       │
                    │  2. mappings/domain_resolver.py                   │
                    │     manager.function (Production) → domain (M&S)  │
                    │  3. mappings/mns_mapping.py                       │
                    │     BU + cluster resolution                       │
                    │  4. sql/ppl/manager_kpis.sql → Dal.fetch_data()  │
                    │  5. sql/mns/site_kpis.sql   → Dal.fetch_data()  │
                    │  6. preprocessing/percentiles.py → peer ranking   │
                    │  7. preprocessing/anomaly_flags.py → flag outliers│
                    │                                                  │
                    └──────────────────────────────────────────────────┘
                │
                ▼
LLM receives: { profile: {...}, people_kpis: [...flagged...], business_kpis: [...flagged...] }
```

This follows the same pattern as `fin_cross_sell_category` (builds SQL internally via `Dal`,
applies fuzzy matching, returns clean JSON) and `mns_pulse` (calls Data API internally,
returns aggregated data).

---

## Schemas — where they live

| Schema | Lives in | Purpose |
|--------|----------|---------|
| `BillboardTemplate`, `DeepDive`, `TextSentiment`, etc. | **`aily-agent`** (`aily_agent/schemas/output_templates/`) | Frontend template types — imported by agent-lab |
| `PrioritiesOutput` (Pydantic) | **`aily-agent-lab`** (`aily_agent_team_productivity/agent/schemas.py`) | Structured LLM output — what `run_question()` returns |

```python
# In team_productivity_agent.py:
from aily_agent.schemas.output_templates.templates.community.billboard import (
    BillboardTemplate, BillboardDetail, DeepDive    # from aily-agent repo
)
from .schemas import PrioritiesOutput                # local to this agent
```

The agent gets `PrioritiesOutput` from the LLM, then maps it to `BillboardTemplate` for the frontend.

---

## Adding a new domain

Adding GTM, Finance, R&D... = SQL + config + mapping. **Tool signatures don't change.**

```
1. Add sql/gtm/pipeline_kpis.sql
2. Add configs/domains/gtm.yaml          (metrics, table, relevant_functions)
3. Add mappings/gtm_mapping.py           (how managers map to GTM KPIs)
4. Update function_to_domain.yaml        (Sales → GTM, Commercial → GTM)
5. Done. get_manager_kpis auto-detects the new domain.
```

```
configs/domains/mns.yaml       configs/domains/gtm.yaml       configs/domains/...
         │                              │                              │
         └──────────────┬───────────────┘──────────────────────────────┘
                        ▼
              domain_resolver.py              ← INSIDE the tool
              manager.function → relevant domains
                        │
                        ▼
              sql/{domain}/ → Dal → preprocessing → flags
                        │
                        ▼
              get_manager_kpis returns: people_kpis[] + business_kpis[]
                        │
                        ▼
              connections/correlations.py → cross-domain analysis   ← INSIDE detect_cross_domain_correlations
                        │
                        ▼
              LLM loop (skill) → priorities   ← HERE is where the LLM reasons
```

---

## vs Performance Agent (`aily-agent-ppl`)

| | Performance Agent | Productivity Agent (this) |
|---|---|---|
| Focus | People KPIs + surveys | People × Business KPIs |
| Intelligence | SHAP/DiCE + LLM refinement | LLM agent loop with MCP tools |
| Extensibility | Self-contained | New domains plug in |
| Superagent | Not accessible | Same skill + tools |
| Replaces? | — | No, complementary |
