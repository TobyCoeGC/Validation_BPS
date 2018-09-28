SELECT 
'Revenue_Share - Difference between Prep SUM and Report SUM detected' as msg
FROM

(SELECT CASE WHEN 
      ROUND(st/cnt,2) <> ROUND(divr,2) THEN 1 ELSE NULL END AS check
      FROM
      (

            SELECT
              SUM(total) AS st
            , COUNT(*)   AS cnt
            , MAX(total) AS divr
            FROM
            ( -- if we remove or add lines in here then the check remains dynamic - add 0.01 to any value and it will detect, add 0.0049 and it is just outside the required precision
              SELECT ROUND(SUM(revenue_share),2) AS total FROM `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_kpi_revenue_prep`
              UNION ALL
              SELECT ROUND(SUM(revenue_share),2) AS total FROM `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.vw_kpi_revenue_prep`
              UNION ALL
              SELECT ROUND(SUM(revenue_share),2)+0.0049 AS total FROM `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.experimental_tables.vw_growth_kpi_data_by_partner` 
            )
      ) 
) reconcile
JOIN
(SELECT 1 AS joinr) errors
ON
reconcile.check = errors.joinr
