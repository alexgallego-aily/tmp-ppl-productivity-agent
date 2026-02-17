-- manager_domain_kpis.sql
-- MNS domain KPIs relevant to a specific manager.
-- Three sources via UNION ALL (no CTEs — Redshift optimises better):
--   1. Business-unit KPIs per cluster, optionally filtered
--   2. Country Organisation KPIs filtered by manager's geo_codes
--   3. Business-unit KPIs aggregated across all clusters
--
-- Parameters (filled by Python):
--   business_unit      — e.g. 'General Medicine'
--   bu_cluster_filter  — e.g. "AND c.cluster_label IN ('Dupixent','China')"
--                         or empty string for all clusters
--   geo_list           — e.g. 'France','Germany','Italy'
--   date_filter        — e.g. "AND k.kpi_facts_date >= '2023-01-01'"
--                         or empty string for no limit

-- 1. BU-specific KPIs per cluster
SELECT
    k.kpi_code,
    '{business_unit}' AS business_unit_label,
    c.cluster_label,
    k.kpi_facts_date,
    SUM(k.kpi_facts_numerator)
        / NULLIF(SUM(k.kpi_facts_denominator), 0) AS kpi_value,
    SUM(k.kpi_facts_target_numerator)
        / NULLIF(SUM(k.kpi_facts_target_denominator), 0) AS target_value,
    'domain' AS source
FROM data_normalized.mns_kpi_facts k
JOIN data_normalized.mns_business_units b
    ON k.business_unit_code = b.business_unit_code
LEFT JOIN data_normalized.mns_clusters c
    ON k.cluster_code = c.cluster_code
WHERE (CASE
    WHEN b.business_unit_label IN ('GENMED', 'General Medicine')
        THEN 'General Medicine'
    ELSE b.business_unit_label
END) = '{business_unit}'
{bu_cluster_filter}
{date_filter}
GROUP BY k.kpi_code, c.cluster_label, k.kpi_facts_date

UNION ALL

-- 2. Country Organisation KPIs matching manager's geo_codes
SELECT
    k.kpi_code,
    b.business_unit_label,
    c.cluster_label,
    k.kpi_facts_date,
    SUM(k.kpi_facts_numerator)
        / NULLIF(SUM(k.kpi_facts_denominator), 0) AS kpi_value,
    SUM(k.kpi_facts_target_numerator)
        / NULLIF(SUM(k.kpi_facts_target_denominator), 0) AS target_value,
    'country_org' AS source
FROM data_normalized.mns_kpi_facts k
JOIN data_normalized.mns_business_units b
    ON k.business_unit_code = b.business_unit_code
JOIN data_normalized.mns_clusters c
    ON k.cluster_code = c.cluster_code
WHERE b.business_unit_label LIKE 'Country Organisation%%'
AND c.cluster_label IN ({geo_list})
{date_filter}
GROUP BY k.kpi_code, b.business_unit_label, c.cluster_label, k.kpi_facts_date

UNION ALL

-- 3. BU aggregate (all clusters summed per date)
SELECT
    k.kpi_code,
    '{business_unit}' AS business_unit_label,
    '__BU_AGGREGATE__' AS cluster_label,
    k.kpi_facts_date,
    SUM(k.kpi_facts_numerator)
        / NULLIF(SUM(k.kpi_facts_denominator), 0) AS kpi_value,
    SUM(k.kpi_facts_target_numerator)
        / NULLIF(SUM(k.kpi_facts_target_denominator), 0) AS target_value,
    'bu_aggregate' AS source
FROM data_normalized.mns_kpi_facts k
JOIN data_normalized.mns_business_units b
    ON k.business_unit_code = b.business_unit_code
WHERE (CASE
    WHEN b.business_unit_label IN ('GENMED', 'General Medicine')
        THEN 'General Medicine'
    ELSE b.business_unit_label
END) = '{business_unit}'
{date_filter}
GROUP BY k.kpi_code, k.kpi_facts_date

ORDER BY source, business_unit_label, kpi_code, cluster_label, kpi_facts_date
