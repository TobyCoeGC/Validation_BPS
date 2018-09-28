SELECT
  Label                                                                        AS ColumnName
, 'Amount'                                                                     AS Type
, ROUND(ABS(FlatSUM-SplitSum),2)                                               AS Diff
, CASE
       WHEN FlatSUM-SplitSum>0
           THEN 'FlatSum is Larger than SplitSum - Null Joins likely'
       ELSE 'SplitSum is Larger than FlatSum - Double Counting likely'
  END                                                                          AS comment

FROM
(
    SELECT
      Label
    , ROUND(SUM(ROUND(SumByKpiDayxOrg_split,2)),2)                 AS SplitSUM
    , ROUND(SUM(ROUND(SumByKpiDayxOrg__flat,2)) ,2)                AS FlatSUM
    FROM
    `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.Growth_KPI_Row_by_row_Columns_out_Agg_vs_original_Detail`
    WHERE
    label = 'sign_ups' 
    GROUP BY
    label
)


UNION ALL


    SELECT
       Label                                                                       AS ColumnName
    , 'Volume'                                                                     AS Type
    ,  COUNT(organisation_id)                                                      AS Diff
    , 'N/A'                                                                        AS Comment
    FROM
    `{{ params.gbq_project_id }}.{{ params.gbq_dataset_materialized_views }}.Growth_KPI_Row_by_row_Columns_out_Agg_vs_original_Detail`
    WHERE
    label = 'sign_ups'
    GROUP BY
    label
    ORDER BY ColumnName
