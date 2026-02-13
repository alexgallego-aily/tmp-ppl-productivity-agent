-- managers_summary.sql
-- Resumen mensual de managers con KPIs de equipo agregados.
-- Solo managers Level 3+ con >5 reportes directos fulltime regular.
-- Fuente: data_normalized.ppl_employees + data_normalized.ppl_moves (rolling exits)
-- Campos extraidos del JSON `extra`: GBU_Level_1, Location, Retention, Loss_Impact

WITH qualified_managers AS (
    SELECT DISTINCT e.manager_code, e.geo_code
    FROM data_normalized.ppl_employees e
    INNER JOIN data_normalized.ppl_employees m
        ON e.manager_code = m.employee_code
        AND e.snapshot_date = m.snapshot_date
    WHERE e.snapshot_date = (SELECT MAX(snapshot_date) FROM data_normalized.ppl_employees)
        AND e.is_artificial_record = FALSE
        AND m.is_artificial_record = FALSE
        AND m.management_level_code IN ('Level 3', 'Level 4', 'Level 5', 'Exec Level 1', 'Exec Level 2', 'Exec Comm')
        AND e.employee_time_type = 'Full time'
        AND e.employee_type = 'Regular'
    GROUP BY e.manager_code, e.geo_code
    HAVING COUNT(DISTINCT e.employee_code) > 5
),

monthly_exits AS (
    SELECT
        DATE_TRUNC('month', end_employment_date)::DATE AS exit_month,
        manager_code,
        geo_code,
        COUNT(DISTINCT employee_code) AS exits
    FROM data_normalized.ppl_employees
    WHERE end_employment_date IS NOT NULL
        AND end_employment_date < CURRENT_DATE
        AND is_artificial_record = FALSE
        AND (manager_code, geo_code) IN (SELECT manager_code, geo_code FROM qualified_managers)
        AND employee_time_type = 'Full time'
        AND employee_type = 'Regular'
    GROUP BY 1, 2, 3
),

rolling_exits AS (
    SELECT
        exit_month AS month,
        manager_code,
        geo_code,
        SUM(exits) OVER (
            PARTITION BY manager_code, geo_code
            ORDER BY exit_month
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS exits_rolling_12m
    FROM monthly_exits
)

SELECT
    DATE_TRUNC('month', e.snapshot_date)::DATE AS month,
    e.manager_code,
    e.geo_code,

    -- GBU Level 1 (sin nombre entre paréntesis)
    MODE() WITHIN GROUP (ORDER BY TRIM(REGEXP_REPLACE(
        SPLIT_PART(SPLIT_PART(e.extra, '"GBU_Level_1": "', 2), '"', 1),
        '\s*\([^)]*\)\s*$', ''
    ))) AS primary_gbu_level_1,

    -- Location desglosado: country / cluster / site
    MODE() WITHIN GROUP (ORDER BY TRIM(SPLIT_PART(
        SPLIT_PART(SPLIT_PART(e.extra, '"Location": "', 2), '"', 1),
        ' / ', 1
    ))) AS location_country,

    MODE() WITHIN GROUP (ORDER BY TRIM(SPLIT_PART(
        SPLIT_PART(SPLIT_PART(e.extra, '"Location": "', 2), '"', 1),
        ' / ', 2
    ))) AS location_cluster,

    MODE() WITHIN GROUP (ORDER BY TRIM(SPLIT_PART(
        SPLIT_PART(SPLIT_PART(e.extra, '"Location": "', 2), '"', 1),
        ' / ', 3
    ))) AS location_site,

    -- Function
    MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(REGEXP_REPLACE(
        SPLIT_PART(e.job_unit_code, '>', 1), '\s+', ' '
    )))) AS primary_function,

    -- KPI 1: Headcount
    COUNT(DISTINCT e.employee_code) AS headcount,
    SUM(e.employee_fte) AS total_fte,
    ROUND(AVG(e.employee_fte), 2) AS avg_fte,

    -- KPI 2: Attrition
    COALESCE(r.exits_rolling_12m, 0) AS exits_rolling_12m,
    ROUND(COALESCE(r.exits_rolling_12m, 0) * 100.0
          / NULLIF(COUNT(DISTINCT e.employee_code), 0), 1) AS attrition_rate_12m_pct,

    -- KPI 3: Age
    ROUND(AVG(e.employee_age), 1) AS avg_age,
    MEDIAN(e.employee_age) AS median_age,
    MIN(e.employee_age) AS min_age,
    MAX(e.employee_age) AS max_age,
    ROUND(STDDEV(e.employee_age), 1) AS age_std_dev,

    -- KPI 4: Salary
    MAX(e.employee_salary_uom) AS currency,
    COUNT(DISTINCT CASE WHEN e.employee_salary_uom_value IS NOT NULL
                        THEN e.employee_code END) AS headcount_with_salary,
    ROUND(MEDIAN(e.employee_salary_uom_value), 0) AS median_salary,
    ROUND(AVG(e.employee_salary_uom_value), 0) AS avg_salary,
    ROUND(MIN(e.employee_salary_uom_value), 0) AS min_salary,
    ROUND(MAX(e.employee_salary_uom_value), 0) AS max_salary,

    -- KPI 6: Diversity - Gender
    ROUND(SUM(CASE WHEN e.employee_gender = 'Female' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_female,
    ROUND(SUM(CASE WHEN e.employee_gender = 'Male' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS pct_male,
    COUNT(DISTINCT e.employee_gender) AS distinct_genders,

    -- KPI 7: Diversity - Race/Ethnicity
    COUNT(DISTINCT e.employee_race) AS distinct_races,
    ROUND(COUNT(DISTINCT e.employee_race) * 100.0 / COUNT(*), 1) AS race_diversity_index,

    -- KPI 8: Retention Risk Distribution
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Retention": "', 2), '"', 1) = 'High'
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_high_retention_risk,
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Retention": "', 2), '"', 1) = 'Medium'
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_medium_retention_risk,
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Retention": "', 2), '"', 1) = 'Low'
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_low_retention_risk,

    -- KPI 9: Loss Impact Distribution
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Loss_Impact": "', 2), '"', 1) = 'Critical'
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_critical_loss_impact,
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Loss_Impact": "', 2), '"', 1) = 'High'
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_high_loss_impact,
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Loss_Impact": "', 2), '"', 1) = 'Medium'
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_medium_loss_impact,
    ROUND(SUM(CASE WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Loss_Impact": "', 2), '"', 1) = 'Low'
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_low_loss_impact,

    -- KPI 10: Combined Critical Flight Risk
    ROUND(SUM(CASE
        WHEN SPLIT_PART(SPLIT_PART(e.extra, '"Retention": "', 2), '"', 1) = 'High'
         AND SPLIT_PART(SPLIT_PART(e.extra, '"Loss_Impact": "', 2), '"', 1) IN ('Critical', 'High')
        THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_critical_flight_risk,

    -- KPI 5: Team Health Score components
    ROUND(SUM(CASE WHEN e.has_development_items THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS development_score,

    100 - ROUND(
        SUM(CASE WHEN e.is_ready_for_promotion
                  AND (e.snapshot_date - e.position_start_date) / 365.0 > 1
                 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    ) AS mobility_score,

    100 - COALESCE(
        ROUND(
            SUM(CASE WHEN e.has_no_candidate_for_replacement AND e.is_manager
                     THEN 1 ELSE 0 END) * 100.0
            / NULLIF(SUM(CASE WHEN e.is_manager THEN 1 ELSE 0 END), 0),
            1
        ),
        0
    ) AS succession_score,

    -- Overall Health Score
    ROUND(
        (
            COALESCE(SUM(CASE WHEN e.has_development_items THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0)
          + (100 - COALESCE(SUM(CASE WHEN e.is_ready_for_promotion
                                      AND (e.snapshot_date - e.position_start_date) / 365.0 > 1
                                     THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0))
          + (100 - COALESCE(SUM(CASE WHEN e.has_no_candidate_for_replacement AND e.is_manager
                                     THEN 1 ELSE 0 END) * 100.0
                            / NULLIF(SUM(CASE WHEN e.is_manager THEN 1 ELSE 0 END), 0), 0))
        ) / 3.0,
        1
    ) AS team_health_score

FROM data_normalized.ppl_employees e
LEFT JOIN rolling_exits r
    ON DATE_TRUNC('month', e.snapshot_date)::DATE = r.month
    AND e.manager_code = r.manager_code
    AND e.geo_code = r.geo_code
WHERE e.is_artificial_record = FALSE
    AND (e.manager_code, e.geo_code) IN (SELECT manager_code, geo_code FROM qualified_managers)
    AND e.employee_time_type = 'Full time'
    AND e.employee_type = 'Regular'
GROUP BY 1, 2, 3, r.exits_rolling_12m
HAVING COUNT(DISTINCT e.employee_code) > 5
ORDER BY 2, 3, 1
