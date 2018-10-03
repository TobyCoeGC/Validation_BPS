SELECT COUNT(*)
, organisation_id
, kpi_day
, partner_id
, scheme
FROM
`{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_BPS`
GROUP BY
  organisation_id
, kpi_day
, partner_id
, scheme
HAVING COUNT(*) >1
