-- list_managers.sql
-- Bulk list of manager candidates for batch processing (e.g. causal survey).
-- Parameters: where_clause (e.g. "AND is_manager = TRUE"), limit (e.g. 500).

SELECT DISTINCT
    employee_code,
    management_level_code,
    geo_code,
    is_manager,
    employees_managed
FROM data_normalized.ppl_employees
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM data_normalized.ppl_employees)
  AND is_artificial_record = FALSE
  {where_clause}
ORDER BY employees_managed DESC NULLS LAST, employee_code
LIMIT {limit}
