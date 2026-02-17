-- manager_profile.sql
-- Lightweight profile: manager's own record + context from subordinates.
-- Parameter: manager_code
--
-- Returns columns for both the manager's self info AND aggregated
-- context from their direct reports (GBU, L2, L3, function).

WITH latest_snap AS (
    -- No filter here so Redshift can use sort-key for MAX
    SELECT MAX(snapshot_date) AS snap
    FROM data_normalized.ppl_employees
)

SELECT
    -- Manager's own info
    me.employee_code,
    me.management_level_code,
    me.geo_code,
    me.is_manager,
    me.employees_managed,
    TRIM(SPLIT_PART(SPLIT_PART(me.extra::TEXT, '"Location": "', 2), '"', 1))
        AS location,
    TRIM(REGEXP_REPLACE(
        SPLIT_PART(SPLIT_PART(me.extra::TEXT, '"GBU_Level_1": "', 2), '"', 1),
        '\s*\([^)]*\)\s*$', ''
    )) AS gbu_level_1,
    SPLIT_PART(SPLIT_PART(me.extra::TEXT, '"Level_02_From_Top": "', 2), '"', 1)
        AS level_02_from_top,
    SPLIT_PART(SPLIT_PART(me.extra::TEXT, '"Level_03_From_Top": "', 2), '"', 1)
        AS level_03_from_top,
    UPPER(TRIM(REGEXP_REPLACE(
        SPLIT_PART(me.job_unit_code, '>', 1), '\s+', ' '
    ))) AS primary_function,

    -- Aggregated context from direct reports (more reliable for domain detection)
    MODE() WITHIN GROUP (ORDER BY TRIM(REGEXP_REPLACE(
        SPLIT_PART(SPLIT_PART(rpt.extra::TEXT, '"GBU_Level_1": "', 2), '"', 1),
        '\s*\([^)]*\)\s*$', ''
    ))) AS reports_gbu_level_1,
    MODE() WITHIN GROUP (ORDER BY TRIM(REGEXP_REPLACE(
        SPLIT_PART(SPLIT_PART(rpt.extra::TEXT, '"GBU_Level_2": "', 2), '"', 1),
        '\s*\([^)]*\)\s*$', ''
    ))) AS reports_gbu_level_2,
    MODE() WITHIN GROUP (ORDER BY TRIM(REGEXP_REPLACE(
        SPLIT_PART(SPLIT_PART(rpt.extra::TEXT, '"GBU_Level_3": "', 2), '"', 1),
        '\s*\([^)]*\)\s*$', ''
    ))) AS reports_gbu_level_3,
    MODE() WITHIN GROUP (ORDER BY
        SPLIT_PART(SPLIT_PART(rpt.extra::TEXT, '"Level_02_From_Top": "', 2), '"', 1)
    ) AS reports_level_02,
    MODE() WITHIN GROUP (ORDER BY
        SPLIT_PART(SPLIT_PART(rpt.extra::TEXT, '"Level_03_From_Top": "', 2), '"', 1)
    ) AS reports_level_03,
    MODE() WITHIN GROUP (ORDER BY
        SPLIT_PART(SPLIT_PART(rpt.extra::TEXT, '"Level_04_From_Top": "', 2), '"', 1)
    ) AS reports_level_04,

    COUNT(DISTINCT rpt.employee_code) AS direct_report_count

FROM latest_snap ls
JOIN data_normalized.ppl_employees me
    ON me.employee_code = '{manager_code}'
    AND me.snapshot_date = ls.snap
    AND me.is_artificial_record = FALSE
LEFT JOIN data_normalized.ppl_employees rpt
    ON rpt.manager_code = '{manager_code}'
    AND rpt.snapshot_date = ls.snap
    AND rpt.is_artificial_record = FALSE
    AND rpt.employee_time_type = 'Full time'
    AND rpt.employee_type = 'Regular'
GROUP BY
    me.employee_code, me.management_level_code, me.geo_code,
    me.is_manager, me.employees_managed, me.extra, me.job_unit_code
