# Implementation Plan: PPL → aily-mcp + aily-agent-lab

> Migrating from the `ppl/` prototype to production across two repos.

---

## Overview

```
ppl/ (this repo)          →  aily-mcp (tools + skill)  +  aily-agent-lab (agent + templates)
─────────────────              ──────────────────           ──────────────────────────────
src/data.py functions          MCP tools (5)                TeamProductivityAgent
src/plots.py                   SKILL.md (1)                 BillboardTemplates
src/config.py                  configs/                     data_pipeline/ (optional)
sql/*.sql                      sql/ (embedded)              runners.py + cli.py
main.py (interactive)          prompts/                     deploy_agent()
```

---

## Part 1: aily-mcp — MCP Server + Skill

### 1.1 New MCP Server: `aily-mcp-ppl`

**Location:** `packages/external/ppl/`

**Structure:**
```
packages/external/ppl/
├── pyproject.toml
├── README.md
├── src/
│   └── aily_mcp_ppl/
│       ├── __init__.py
│       ├── main.py                    # Server entry point
│       ├── utils.py                   # Shared helpers (resolve_business_unit, suggest_kpi_mapping)
│       ├── tools/
│       │   ├── find_manager.py        # Tool: find_manager
│       │   ├── manager_profile.py     # Tool: get_manager_profile
│       │   ├── manager_kpis.py        # Tool: get_manager_kpis
│       │   ├── correlations.py        # Tool: detect_cross_domain_correlations
│       │   ├── kpi_drivers.py         # Tool: get_kpi_drivers (future)
│       │   └── prompts/
│       │       ├── find_manager.txt
│       │       ├── manager_profile.txt
│       │       ├── manager_kpis.txt
│       │       └── correlations.txt
│       ├── sql/                        # Embedded SQL queries
│       │   ├── find_manager.sql
│       │   ├── manager_profile.sql
│       │   ├── manager_active_teams.sql
│       │   ├── manager_team_kpis.sql
│       │   └── manager_domain_kpis.sql
│       └── configs/                    # KPI mapping rules, panel defs
│           └── kpi_config.yaml
└── tests/
    ├── test_imports.py
    ├── test_integration.py
    └── integration_suite.yaml
```

### 1.2 Tools (5)

Each tool follows aily-mcp conventions: `@tool_metadata()`, `@mcp_tool_exception_handler`, returns `CallToolResult`.

**Important**: aily-mcp tools use Data API (`AilyAPIClient`), not direct SQL via DAL. The SQL queries need to be migrated to Data API calls, or a new pattern established if direct DAL access is approved for this server.

| # | Tool name | Input params | Returns | Maps to `ppl/` function |
|---|-----------|-------------|---------|------------------------|
| 1 | `find_manager` | `geo_code?, management_level?, location?, gbu?, include_non_managers?` | List of candidates (code, geo, level, location, gbu) | `find_manager()` |
| 2 | `get_manager_profile` | `manager_code` | Profile dict: active teams, geo_codes, kpi_mapping, org fields | `get_manager_profile()` |
| 3 | `get_manager_kpis` | `manager_code, kpi_mapping?, lookback_years?` | PPL KPIs (per team + aggregated) + Domain KPIs + flags | `load_manager_team_kpis()` + `load_manager_domain_kpis()` + `apply_team_size_filter()` |
| 4 | `detect_cross_domain_correlations` | `manager_code` (or pre-loaded data) | List of significant (MNS KPI, PPL KPI, p, TE, EE, lag) pairs | `run_correlation()` |
| 5 | `get_kpi_drivers` | `manager_code, kpi_name` | Drill-down breakdown for a specific KPI (future) | TBD |

**Data access: DAL + SQL files (validated pattern)**

Convention is Data API, but `rnd_clinops` already uses direct DAL + embedded SQL files extensively (20+ SQL files, `dal.db.fetch_data_as_df(query)`). This is an accepted pattern in aily-mcp when:
- Queries are complex and specific to the tool
- No Data API endpoints exist for the tables
- The tool needs JOINs across multiple tables with dynamic filters

**We follow the `rnd_clinops` pattern:** embed SQL in `sql/`, load with `format()`, execute via DAL. The SQL from `ppl/sql/` migrates directly.

### 1.3 Skill: `ppl_productivity`

**Location:** `packages/internal/skills/src/aily_mcp_skills/skills/external/ppl/ppl_productivity/SKILL.md`

**Purpose:** Tells the LLM how to use the PPL tools, in what order, and how to synthesize findings into priorities.

**Draft structure:**

```yaml
---
name: ppl_productivity
description: "Use when analyzing a manager's people metrics and their impact on business performance. Covers team KPIs (headcount, attrition, tenure, health, risk), domain KPI correlation, and priority recommendations."
metadata:
  version: 1.0.0
allowed_tools:
  - find_manager
  - get_manager_profile
  - get_manager_kpis
  - detect_cross_domain_correlations
---
```

**Skill body should include:**

1. **Workflow**: find → profile → kpis → correlations → synthesize priorities
2. **KPI interpretation rules**: What each PPL KPI means, what's "good" vs "bad"
3. **Correlation interpretation**: How to read TE/EE/lag, signal strength thresholds
4. **Priority format**: Structure for output (driver, business impact, action, lag)
5. **Privacy rules**: Never expose individual employee data, always aggregate
6. **Team size rules**: <5 people → aggregate only, don't show individual team breakdown

**Note**: Educational/explanatory content (KPI glossary, methodology, etc.) does NOT go
in the skill. It goes in the **Agent Context** (`create_agent_context()`) in aily-agent-lab.
See section 2.5.

### 1.4 Superagent integration

**Ref:** [How to Bring Your Agent into the Super Agent](https://ailylabs.atlassian.net/wiki/spaces/AIL/pages/3682172934/How+to+Bring+Your+Agent+into+the+Super+Agent)

The superagent loads the `ppl_productivity` skill via `get_skills()` and gains access to the
same MCP tools. Two modes of interaction:

**a) Analytical queries** (tool-calling via skill):
```
User: "Manager abc123 has high attrition. Is it affecting production?"
  → LLM loads skill → calls get_manager_kpis → calls detect_cross_domain_correlations
  → synthesizes answer using skill rules → text response
```

**b) Educational queries** (agent context, no tool calls):
```
User: "What is team health score?"
  → Superagent reads AgentContext from DB (deployed by batch agent)
  → answers from context → no tools called, instant response
```

The educational content (KPI glossary, methodology explanations) lives in the **Agent Context**
(`create_agent_context()` in aily-agent-lab), NOT in the skill. The skill only teaches the LLM
how to USE the tools. The agent context teaches the LLM how to EXPLAIN the domain.

See section 2.4 for Agent Context details.

### 1.5 Configuration

**Migrate from `ppl/src/config.py` to YAML:**
- `KPI_MAPPING_RULES` → `kpi_config.yaml`
- `KPI_MAPPING_LABELS` → `kpi_config.yaml`
- `PPL_CORRELATABLE_KPIS` → `kpi_config.yaml`
- `TEAM_KPI_PANELS` → `kpi_config.yaml`
- `MIN_TEAM_HEADCOUNT` → `kpi_config.yaml`

### 1.6 Registration

**Root `pyproject.toml`** changes:
- Add `aily-mcp-ppl` to `[tool.uv.workspace]`, `[tool.uv.sources]`, and `[project].dependencies`

**Docker / deployment:**
- Add `aily-mcp-ppl` to the MCP server deployment config

---

## Part 2: aily-agent-lab — Agent + Templates

### 2.1 New Agent Package: `aily-agent-team-productivity`

**Location:** `packages/aily-agent-team-productivity/`

**Note:** Distinct from existing `aily-agent-team-performance` (which handles different data/scope).

**Structure:**
```
packages/aily-agent-team-productivity/
├── pyproject.toml
├── README.md
├── CHANGELOG.md
├── aily_agent_team_productivity/
│   ├── __init__.py
│   ├── cli.py
│   ├── runners.py
│   ├── paths.py
│   ├── agent/
│   │   ├── __init__.py
│   │   ├── team_productivity_agent.py    # Main agent class
│   │   ├── publish_agent.py              # get_team_productivity_agent_params()
│   │   ├── templates/
│   │   │   ├── billboard_leaderboard.py  # Priority list overview
│   │   │   └── billboard_detail.py       # Individual priority deep-dive
│   │   ├── context/
│   │   │   └── agent_context.md          # Educational context for superagent chat
│   │   ├── config/
│   │   │   └── agent_config.yaml
│   │   └── utils/
└── tests/
    └── test_imports.py
```

### 2.2 Agent Class

```python
from aily_agent_team_productivity.paths import AGENT_DIR

@AgentRegistry.register("team_productivity")
class TeamProductivityAgent(BaseAgent):
    def __init__(self, manager_code: str, snapshot_date: str | None = None):
        super().__init__()
        self.manager_code = manager_code
        self.snapshot_date = snapshot_date

    def run(self):
        # 1. Agent.run() creates MCPConfig pointing to aily-mcp-ppl + aily-mcp-skills
        # 2. LLM loads ppl_productivity skill
        # 3. LLM calls tools in sequence:
        #    get_manager_profile → get_manager_kpis → detect_cross_domain_correlations
        # 4. LLM synthesizes 1-4 priorities using skill rules
        # 5. Agent wraps LLM output in BillboardTemplates
        return (
            self.create_agent_templates(),
            self.create_agent_structure(),
            self.create_agent_metadata(),
        )

    def create_agent_context(self) -> AgentContext:
        context_md = (AGENT_DIR / "context" / "agent_context.md").read_text()
        return AgentContext(agent_context=context_md)
```

**Architecture: Agentic (Option D) — tools always compute, batch paraleliza**

The principle: **same tools, same skill, same LLM — both batch and superagent.**

```
Batch:       deploy_agent(parallel=True, workers=10)
             → each pod: Agent.run() → LLM → calls MCP tools live → synthesizes → templates
             → saves templates + agent_context to DB

Superagent:  User question → LLM reads AgentContext (educational layer from DB)
             → if analytical: calls MCP tools (cache hit → fast, miss → compute live)
             → if educational: answers from AgentContext directly (no tool calls)
```

- **Batch**: runs monthly, doesn't need to be fast. Parallelized across K8s pods.
- **Superagent**: reads from batch cache for managers already processed. Computes live for ad-hoc queries (e.g., different lag, new manager).
- **One codebase**: computation logic lives in MCP tools only. No duplicate pipeline logic.
- **aily-ai-correlator**: added as dependency of `aily-mcp-ppl` (available from CloudSmith `>=2.0.1`).

### 2.3 Templates

**Billboard Leaderboard** (overview screen):
- Title: "People × Business Priorities"
- Subtitle: Manager scope (teams, countries, headcount)
- Items: 1-4 priorities, each with:
  - Heading: Business impact (e.g. "OTIF declining")
  - Subtext: People driver (e.g. "Role stagnation detected")
  - Sentiment: red/amber/green based on signal strength

**Billboard Detail** (per priority):
- Title: Priority name
- Body: LLM-generated narrative
  - What's happening (the correlation finding)
  - Why it matters (business impact)
  - What to do (recommended action)
  - Timeline (lag → when to expect results)

### 2.4 Agent Context (`create_agent_context()`)

**Ref:** [How to Bring Your Agent into the Super Agent](https://ailylabs.atlassian.net/wiki/spaces/AIL/pages/3682172934/How+to+Bring+Your+Agent+into+the+Super+Agent)

The `AgentContext` is a markdown string saved to DB alongside the agent. The superagent reads
it when users chat about this agent's domain. It's the **educational layer** — NOT the skill
(which teaches the LLM how to use tools).

**Implementation:**
```python
def create_agent_context(self) -> AgentContext:
    context_path = AGENT_DIR / "context" / "agent_context.md"
    return AgentContext(agent_context=context_path.read_text())
```

**`agent_context.md` should contain:**

```markdown
# People × Business Intelligence — Agent Context

## What this agent does
Analyzes the relationship between People (HR) metrics and Business (M&S) KPIs
for a specific manager's organization. Identifies which people factors are
leading indicators of business performance, and recommends actions.

## PPL KPI Glossary

### Size & Capacity
- **headcount**: Number of active employees in the team/org
- **attrition_rate_pct**: % of employees who left in the last 12 months (rolling)

### Team Health
- **team_health_score**: Composite (0-100), average of development + mobility +
  succession scores. Below 60 = concerning. Above 80 = healthy.
- **development_score**: % of employees with active development plans (training,
  career growth). Directly measures investment in people.
- **mobility_score**: 100 minus % of promotion-ready employees stuck in position >1yr.
  Low = people ready to move but not moving.
- **succession_score**: 100 minus % of managers with no replacement candidate.
  Low = leadership continuity risk.

### Tenure & Stability
- **pct_long_in_position**: % of employees in the same role >4 years.
  High = stagnation risk (complacency, reduced agility).
- **avg_tenure_years**: Average years in the company.
- **avg_time_in_position_years**: Average years in current role.

### Risk
- **pct_high_retention_risk**: % flagged as high retention risk by HR assessment.
- **pct_critical_flight_risk**: % who are both high retention risk AND high loss impact.

[... continue for all 18 KPIs ...]

## MNS KPI Glossary
- **OTIF** (On Time In Full): % of orders delivered on time and complete. The key
  supply chain metric.
- **Discards**: Production waste — batches that fail quality and must be scrapped.
- **Supply Plan Accuracy (SPA)**: How well the production plan matches actual demand.

## Correlation Methodology
The agent uses Granger Causality and Transfer Entropy to detect causal-like
relationships between People and Business KPIs:

- **Direction**: Always PPL (cause) → MNS (effect). "Did a people change predict
  a business outcome?"
- **Lag**: Months between the people signal and business impact (e.g., lag=3
  means people metric changed 3 months before business metric reacted)
- **Signal strength**:
  - p < 0.05: Statistically significant (Granger test passed)
  - TE > 0.1: Moderate information flow from people to business
  - TE > 0.3: Strong causal-like signal
- **Limitation**: The test detects temporal precedence and information flow,
  not guaranteed causation. It does not indicate if the relationship is
  positive or negative — only that it exists.

## Privacy Rules
- All data is aggregated. No individual employee data is ever exposed.
- Teams with fewer than 5 people are only shown in aggregate view.
- Manager codes are anonymized hashes.
```

This context allows the superagent to answer questions like:
- "What is team health score?" → reads glossary
- "How is causality calculated?" → reads methodology section
- "What does a lag of 6 months mean?" → reads correlation methodology

Without calling any MCP tool — pure context-based answers.

### 2.5 Multi-Agent Deployment

```python
def get_team_productivity_agent_params(snapshot_date=None):
    """One agent per eligible manager."""
    dal = Dal()
    # Query: managers with enough data (>=12 months, >=5 headcount)
    managers = dal.db.fetch_data_as_df(query="""
        SELECT DISTINCT manager_code
        FROM data_normalized.ppl_employees
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ...)
          AND is_artificial_record = FALSE
        -- filter for managers with enough teams/data
    """)
    return [{"manager_code": m} for m in managers["manager_code"]]
```

### 2.6 Data Pipeline (if hybrid approach)

**Pre-computes for each manager:**
1. PPL KPIs (aggregated + per team with size filter)
2. Domain KPIs (bu_aggregate + country_org)
3. Correlations (RCA results: significant pairs with p, TE, EE, lag)
4. Saves to `ai.ppl_agent_correlations_outputs` and `ai.ppl_agent_kpis_outputs`

**Steps:**
```
load_managers → compute_kpis → compute_correlations → save_outputs
```

Each step uses `@step()` decorator. Pipeline uses `@pipeline()`. Supports parallel execution per manager.

### 2.7 Registration

**Root `pyproject.toml`:**
- Add `aily-agent-team-productivity` to workspace members and sources

**Airflow DAG** (in `aily-data-airflow`):
- Schedule: monthly (after PPL data load)
- Steps: 1) Run data pipeline, 2) Run agent publish

---

## Part 3: Execution Order

### Phase 1: MCP Server (aily-mcp)
1. Create `packages/external/ppl/` with server scaffold
2. Migrate SQL queries from `ppl/sql/`
3. Implement tools 1-4 (find, profile, kpis, correlations)
4. Write tool prompts
5. Register in root `pyproject.toml`
6. Test locally with `aily-mcp-ppl` server

### Phase 2: Skill (aily-mcp)
7. Create `ppl_productivity/SKILL.md`
8. Define workflow, interpretation rules, priority format
9. Test with superagent (manual queries)

### Phase 3: Agent (aily-agent-lab)
10. Create `packages/aily-agent-team-productivity/` scaffold
11. Implement data pipeline (if hybrid)
12. Implement agent class with template generators
13. Implement runners + CLI
14. Test `team-productivity-agent-publish` locally
15. Test end-to-end: pipeline → agent → templates → frontend

### Phase 4: Production
16. Merge to main → Docker images built
17. Create Airflow DAG for monthly batch
18. Promote to UAT → Prod

---

## Resolved Questions

1. **Data API vs DAL**: **DAL + SQL files**. `rnd_clinops` already uses this pattern in production (20+ SQL files, `dal.db.fetch_data_as_df`). Our queries are complex multi-table JOINs with no existing Data API endpoints. We follow the same pattern.

2. **aily-ai-correlator dependency**: External package from CloudSmith (`>=2.0.1`). Added as dependency of `aily-mcp-ppl`. Computation happens live in the MCP tool `detect_cross_domain_correlations`. Batch paralelizes across pods; superagent reads from batch cache when available, computes live otherwise.

3. **Frontend chart support**: **Text only** for now. Billboard templates support text, sentiment, and tags — no embedded charts. The correlation findings are conveyed as text with signal strength indicators (sentiment colors). Visual charts remain in the interactive CLI / superagent responses only.

## Open Questions

1. **Agent naming**: `team-productivity` vs something else? Must not collide with existing `team-performance`.

2. **Manager eligibility**: What's the minimum data requirement to generate a meaningful report? (e.g., >= 12 months of PPL data, >= 5 headcount, MNS domain mapping exists)

3. **Scope**: Start with M&S only (General Medicine, Vaccines, Specialty Care)? Or all domains from day one?

4. **Privacy**: The `find_manager` tool returns anonymized hashes. Is that sufficient for the LLM, or does it need human-readable names at some point?
