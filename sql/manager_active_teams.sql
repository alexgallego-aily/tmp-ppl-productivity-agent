-- manager_active_teams.sql
-- Active teams for a manager: org_level × geo_code breakdown.
-- Uses USING joins for ppl_positions/ppl_organization_levels.
-- Parameters: manager_code, snapshot_date
--
-- Note: snapshot_date should be chosen by Python to avoid months
-- where positions data hasn't synced yet.

SELECT
    pol.organization_level_code,
    pe.geo_code,
    COUNT(DISTINCT pe.employee_code) AS team_size,
    MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(REGEXP_REPLACE(
        SPLIT_PART(pe.job_unit_code, '>', 1), '\s+', ' '
    )))) AS primary_function
FROM data_normalized.ppl_employees pe
LEFT JOIN data_normalized.ppl_positions pp
    USING (snapshot_date, position_code)
LEFT JOIN data_normalized.ppl_organization_levels pol
    USING (snapshot_date, organization_level_code)
WHERE pe.manager_code = '{manager_code}'
  AND pe.snapshot_date = '{snapshot_date}'
  AND pe.is_artificial_record = FALSE
  AND pe.employee_time_type = 'Full time'
  AND pe.employee_type = 'Regular'
GROUP BY pol.organization_level_code, pe.geo_code
ORDER BY team_size DESC
