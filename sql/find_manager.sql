-- find_manager.sql
-- Lookup candidates for a manager based on descriptive filters.
-- Uses the latest snapshot only.
-- Parameter: where_clause — dynamically built in Python.

SELECT DISTINCT
    employee_code,
    manager_code,
    management_level_code,
    geo_code,
    is_manager,
    employees_managed,

    -- Descriptive fields from extra JSON (cast to TEXT for SPLIT_PART)
    TRIM(SPLIT_PART(SPLIT_PART(extra::TEXT, '"Location": "', 2), '"', 1))
        AS location,
    TRIM(REGEXP_REPLACE(
        SPLIT_PART(SPLIT_PART(extra::TEXT, '"GBU_Level_1": "', 2), '"', 1),
        '\s*\([^)]*\)\s*$', ''
    )) AS gbu_level_1,
    SPLIT_PART(SPLIT_PART(extra::TEXT, '"Level_02_From_Top": "', 2), '"', 1)
        AS level_02_from_top,
    SPLIT_PART(SPLIT_PART(extra::TEXT, '"Level_03_From_Top": "', 2), '"', 1)
        AS level_03_from_top,

    -- Function context
    UPPER(TRIM(REGEXP_REPLACE(
        SPLIT_PART(job_unit_code, '>', 1), '\s+', ' '
    ))) AS primary_function,

    employee_age,
    employee_gender

FROM data_normalized.ppl_employees
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM data_normalized.ppl_employees)
  AND is_artificial_record = FALSE
  {where_clause}
ORDER BY employees_managed DESC NULLS LAST, employee_code
