-- manager_team_kpis.sql
-- Monthly KPIs by team (organization_level_code × geo_code) for one manager.
-- Parameter: manager_code
--
-- Optimised: single scan of ppl_employees with one JOIN pass to
-- ppl_positions/ppl_organization_levels.  Exits are derived from
-- the same base CTE (no second scan).

-- 1. Single scan: all rows for this manager across all snapshots
WITH base AS (
    SELECT
        DATE_TRUNC('month', pe.snapshot_date)::DATE AS month,
        pol.organization_level_code,
        pe.geo_code,
        pe.employee_code,
        pe.management_level_code,
        pe.employee_age,
        pe.employee_gender,
        pe.employee_salary_uom,
        pe.employee_salary_uom_value,
        pe.employee_fte,
        pe.is_manager,
        pe.employees_managed,
        pe.is_ready_for_promotion,
        pe.is_a_succession_candidate,
        pe.has_development_items,
        pe.has_no_candidate_for_replacement,
        pe.end_employment_date,
        pe.job_unit_code,
        pe.extra,
        ROUND(
            ((pe.snapshot_date - COALESCE(pe.continuous_hire_date, pe.hire_date))
             / 365.25)::NUMERIC, 1
        ) AS tenure_years,
        ROUND(
            ((pe.snapshot_date - pe.position_start_date) / 365.25)::NUMERIC, 1
        ) AS time_in_position_years
    FROM data_normalized.ppl_employees pe
    LEFT JOIN data_normalized.ppl_positions pp
        USING (snapshot_date, position_code)
    LEFT JOIN data_normalized.ppl_organization_levels pol
        USING (snapshot_date, organization_level_code)
    WHERE pe.manager_code = '{manager_code}'
      AND pe.is_artificial_record = FALSE
      AND pe.employee_time_type = 'Full time'
      AND pe.employee_type = 'Regular'
),

-- 2. Exits: deduplicate per employee (same employee appears in many snapshots)
distinct_exits AS (
    SELECT DISTINCT
        employee_code,
        organization_level_code,
        geo_code,
        end_employment_date
    FROM base
    WHERE end_employment_date IS NOT NULL
      AND end_employment_date < CURRENT_DATE
),

monthly_exits AS (
    SELECT
        DATE_TRUNC('month', end_employment_date)::DATE AS exit_month,
        organization_level_code,
        geo_code,
        COUNT(DISTINCT employee_code) AS exits
    FROM distinct_exits
    GROUP BY 1, 2, 3
),

rolling_exits AS (
    SELECT
        exit_month AS month,
        organization_level_code,
        geo_code,
        SUM(exits) OVER (
            PARTITION BY organization_level_code, geo_code
            ORDER BY exit_month
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS exits_rolling_12m
    FROM monthly_exits
)

-- 3. Aggregate KPIs per team per month
SELECT
    e.month,
    e.organization_level_code,
    e.geo_code,

    MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(REGEXP_REPLACE(
        SPLIT_PART(e.job_unit_code, '>', 1), '\s+', ' '
    )))) AS primary_function,

    MODE() WITHIN GROUP (ORDER BY e.management_level_code) AS primary_mgmt_level,

    -- 1. Size & Capacity
    COUNT(DISTINCT e.employee_code) AS headcount,
    ROUND(SUM(e.employee_fte), 1) AS total_fte,

    -- 2. Attrition
    COALESCE(r.exits_rolling_12m, 0) AS exits_rolling_12m,
    ROUND(COALESCE(r.exits_rolling_12m, 0) * 100.0
          / NULLIF(COUNT(DISTINCT e.employee_code), 0), 1) AS attrition_rate_pct,

    -- 3. Age Profile
    ROUND(AVG(e.employee_age), 1) AS avg_age,
    ROUND(SUM(CASE WHEN e.employee_age >= 55 THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_near_retirement,

    -- 4. Tenure & Position Stability
    ROUND(AVG(e.tenure_years), 1) AS avg_tenure_years,
    ROUND(AVG(e.time_in_position_years), 1) AS avg_time_in_position_years,
    ROUND(SUM(CASE WHEN e.time_in_position_years > 4 THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_long_in_position,

    -- 5. Compensation
    MAX(e.employee_salary_uom) AS currency,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY e.employee_salary_uom_value)::NUMERIC, 0) AS median_salary,

    -- 6. Gender Diversity
    ROUND(SUM(CASE WHEN e.employee_gender = 'Female' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_female,

    -- 7. Talent Pipeline
    ROUND(SUM(CASE WHEN e.is_ready_for_promotion THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_ready_for_promotion,
    ROUND(SUM(CASE WHEN e.is_a_succession_candidate THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_succession_candidates,

    -- 8. Team Health Components
    ROUND(SUM(CASE WHEN e.has_development_items THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS development_score,

    100 - ROUND(
        SUM(CASE WHEN e.is_ready_for_promotion
                  AND e.time_in_position_years > 1
                 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    ) AS mobility_score,

    100 - COALESCE(
        ROUND(
            SUM(CASE WHEN e.has_no_candidate_for_replacement AND e.is_manager
                     THEN 1 ELSE 0 END) * 100.0
            / NULLIF(SUM(CASE WHEN e.is_manager THEN 1 ELSE 0 END), 0), 1
        ), 0
    ) AS succession_score,

    -- Composite health score
    ROUND(
        (
            COALESCE(SUM(CASE WHEN e.has_development_items THEN 1 ELSE 0 END)
                     * 100.0 / COUNT(*), 0)
          + (100 - COALESCE(SUM(CASE WHEN e.is_ready_for_promotion
                                      AND e.time_in_position_years > 1
                                     THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0))
          + (100 - COALESCE(SUM(CASE WHEN e.has_no_candidate_for_replacement
                                      AND e.is_manager
                                     THEN 1 ELSE 0 END) * 100.0
                            / NULLIF(SUM(CASE WHEN e.is_manager
                                              THEN 1 ELSE 0 END), 0), 0))
        ) / 3.0, 1
    ) AS team_health_score,

    -- 9. Risk
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra::TEXT,
          '"Retention": "', 2), '"', 1) = 'High'
          THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_high_retention_risk,

    ROUND(SUM(CASE
        WHEN SPLIT_PART(SPLIT_PART(e.extra::TEXT,
             '"Retention": "', 2), '"', 1) = 'High'
         AND SPLIT_PART(SPLIT_PART(e.extra::TEXT,
             '"Loss_Impact": "', 2), '"', 1) IN ('Critical', 'High')
        THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_critical_flight_risk,

    -- 10. Management Structure
    ROUND(SUM(CASE WHEN e.is_manager THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_managers,
    ROUND(AVG(CASE WHEN e.is_manager AND e.employees_managed > 0
                   THEN e.employees_managed END), 1) AS avg_span_of_control,

    -- 11. Management Level Composition (% per level)
    ROUND(SUM(CASE WHEN e.management_level_code = 'Exec Comm' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_exec_comm,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Exec Level 1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_exec_level_1,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Exec Level 2' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_exec_level_2,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Level 1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_level_1,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Level 2' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_level_2,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Level 3' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_level_3,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Level 4' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_level_4,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Level 5' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_level_5,
    ROUND(SUM(CASE WHEN e.management_level_code = 'Local' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_local

FROM base e
LEFT JOIN rolling_exits r
    ON e.month = r.month
    AND e.organization_level_code = r.organization_level_code
    AND e.geo_code = r.geo_code
GROUP BY e.month, e.organization_level_code, e.geo_code, r.exits_rolling_12m
ORDER BY e.organization_level_code, e.geo_code, e.month
