-- manager_domain_kpis.sql
-- MNS domain KPIs relevant to a specific manager.
-- Three sources from two scans (CTE reuse):
--   1. Business-unit KPIs per cluster, optionally filtered
--   2. Country Organisation KPIs filtered by manager's geo_codes
--   3. Business-unit KPIs aggregated (derived from CTE, no extra scan)
--
-- Parameters (filled by Python):
--   business_unit      — e.g. 'General Medicine'
--   bu_cluster_filter  — e.g. "AND cluster_label IN ('Dupixent','China')"
--                         or empty string for all clusters
--   geo_list           — e.g. 'France','Germany','Italy'
--   date_filter        — e.g. "AND k.kpi_facts_date >= '2023-01-01'"
--                         or empty string for no limit

WITH bu_codes AS (
    SELECT business_unit_code
    FROM data_normalized.mns_business_units
    WHERE (CASE
        WHEN business_unit_label IN ('GENMED', 'General Medicine')
            THEN 'General Medicine'
        ELSE business_unit_label
    END) = '{business_unit}'
),

co_codes AS (
    SELECT business_unit_code
    FROM data_normalized.mns_business_units
    WHERE business_unit_label LIKE 'Country Organisation%%'
),

bu_raw AS (
    SELECT
        k.kpi_code,
        '{business_unit}' AS business_unit_label,
        c.cluster_label,
        k.kpi_facts_date,
        k.kpi_facts_numerator,
        k.kpi_facts_denominator,
        k.kpi_facts_target_numerator,
        k.kpi_facts_target_denominator
    FROM data_normalized.mns_kpi_facts k
    JOIN bu_codes bc ON k.business_unit_code = bc.business_unit_code
    LEFT JOIN data_normalized.mns_clusters c
        ON k.cluster_code = c.cluster_code
    WHERE 1=1 {date_filter}
)

-- 1. BU-specific KPIs per cluster
SELECT
    kpi_code,
    business_unit_label,
    cluster_label,
    kpi_facts_date,
    SUM(kpi_facts_numerator)
        / NULLIF(SUM(kpi_facts_denominator), 0) AS kpi_value,
    SUM(kpi_facts_target_numerator)
        / NULLIF(SUM(kpi_facts_target_denominator), 0) AS target_value,
    'domain' AS source
FROM bu_raw
WHERE 1=1 {bu_cluster_filter}
GROUP BY kpi_code, business_unit_label, cluster_label, kpi_facts_date

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
JOIN co_codes cc ON k.business_unit_code = cc.business_unit_code
JOIN data_normalized.mns_business_units b
    ON k.business_unit_code = b.business_unit_code
JOIN data_normalized.mns_clusters c
    ON k.cluster_code = c.cluster_code
WHERE c.cluster_label IN ({geo_list}) {date_filter}
GROUP BY k.kpi_code, b.business_unit_label, c.cluster_label, k.kpi_facts_date

UNION ALL

-- 3. BU aggregate (reuses CTE — no additional table scan)
SELECT
    kpi_code,
    business_unit_label,
    '__BU_AGGREGATE__' AS cluster_label,
    kpi_facts_date,
    SUM(kpi_facts_numerator)
        / NULLIF(SUM(kpi_facts_denominator), 0) AS kpi_value,
    SUM(kpi_facts_target_numerator)
        / NULLIF(SUM(kpi_facts_target_denominator), 0) AS target_value,
    'bu_aggregate' AS source
FROM bu_raw
GROUP BY kpi_code, business_unit_label, kpi_facts_date

ORDER BY source, business_unit_label, kpi_code, cluster_label, kpi_facts_date
