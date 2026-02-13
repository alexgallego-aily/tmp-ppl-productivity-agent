-- mns_kpis.sql
-- KPIs de Manufacturing & Supply con valores y targets por business unit, cluster y fecha.
-- Fuente: data_normalized.mns_kpi_facts + mns_business_units + mns_clusters
-- Los valores se calculan como numerator / denominator.

SELECT
    k.kpi_code,
    CASE
        WHEN b.business_unit_label IN ('GENMED', 'General Medicine') THEN 'General Medicine'
        ELSE b.business_unit_label
    END AS business_unit_label,
    c.cluster_label,
    k.kpi_facts_date,
    SUM(k.kpi_facts_numerator) / NULLIF(SUM(k.kpi_facts_denominator), 0) AS kpi_value,
    SUM(k.kpi_facts_target_numerator) / NULLIF(SUM(k.kpi_facts_target_denominator), 0) AS target_value
FROM data_normalized.mns_kpi_facts k
LEFT JOIN data_normalized.mns_business_units b
    ON k.business_unit_code = b.business_unit_code
LEFT JOIN data_normalized.mns_clusters c
    ON k.cluster_code = c.cluster_code
GROUP BY
    k.kpi_code,
    CASE
        WHEN b.business_unit_label IN ('GENMED', 'General Medicine') THEN 'General Medicine'
        ELSE b.business_unit_label
    END,
    c.cluster_label,
    k.kpi_facts_date
ORDER BY
    k.kpi_code,
    business_unit_label,
    k.kpi_facts_date
