SELECT 
'Organisation_ID - Difference between BAU and By Partnert Orgaisation Population - SUM detected' as msg
FROM

(SELECT CASE WHEN 
      ROUND(st/cnt,1) <> ROUND(divr,0) THEN 1 ELSE NULL END AS check
      FROM
      (

            SELECT
              SUM(total) AS st
            , COUNT(*)   AS cnt
            , MAX(total) AS divr
            FROM
            ( 
              
              
              SELECT 'Number of ORGS - KPI - BAU' as Label, count(distinct organisation_id) as total FROM `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data` -- base
              UNION ALL
              SELECT 'Number of ORGS - KPI - BPS' as Label, count(distinct organisation_id) as total FROM `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_growth_kpi_data_by_partner` --output
              
            )
      ) 
) reconcile
JOIN
(SELECT 1 AS joinr) errors
ON
reconcile.check = errors.joinr
