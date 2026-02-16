-- manager_domain_kpis.sql
-- MNS domain KPIs relevant to a specific manager (optimised).
-- Two sources combined:
--   1. Business-unit KPIs, optionally filtered to specific clusters
--   2. Country Organisation KPIs filtered by manager's geo_codes
--
-- Parameters (filled by Python):
--   business_unit   — e.g. 'General Medicine'
--   bu_cluster_filter — e.g. "AND c.cluster_label IN ('Dupixent','China')"
--                       or empty string for all clusters
--   geo_list        — e.g. 'France','Germany','Italy'
--
-- Output: one row per (kpi_code, business_unit_label, cluster_label, kpi_facts_date)

SELECT
    k.kpi_code,
    CASE
        WHEN b.business_unit_label IN ('GENMED', 'General Medicine')
            THEN 'General Medicine'
        ELSE b.business_unit_label
    END AS business_unit_label,
    c.cluster_label,
    k.kpi_facts_date,
    SUM(k.kpi_facts_numerator)
        / NULLIF(SUM(k.kpi_facts_denominator), 0) AS kpi_value,
    SUM(k.kpi_facts_target_numerator)
        / NULLIF(SUM(k.kpi_facts_target_denominator), 0) AS target_value
FROM data_normalized.mns_kpi_facts k
LEFT JOIN data_normalized.mns_business_units b
    ON k.business_unit_code = b.business_unit_code
LEFT JOIN data_normalized.mns_clusters c
    ON k.cluster_code = c.cluster_code
WHERE
    (
        -- 1. BU-specific KPIs
        (CASE
            WHEN b.business_unit_label IN ('GENMED', 'General Medicine')
                THEN 'General Medicine'
            ELSE b.business_unit_label
         END) = '{business_unit}'
        {bu_cluster_filter}
    )
    OR
    (
        -- 2. Country Organisation KPIs matching manager's geo_codes
        b.business_unit_label LIKE 'Country Organisation%%'
        AND c.cluster_label IN ({geo_list})
    )
GROUP BY
    k.kpi_code,
    CASE
        WHEN b.business_unit_label IN ('GENMED', 'General Medicine')
            THEN 'General Medicine'
        ELSE b.business_unit_label
    END,
    c.cluster_label,
    k.kpi_facts_date
ORDER BY
    business_unit_label,
    k.kpi_code,
    c.cluster_label,
    k.kpi_facts_date
