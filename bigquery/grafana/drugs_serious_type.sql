-- Top 3 drugs that associated with serious events in older adults
WITH serious_drug_count AS ( 
  SELECT 
    serious_type
    , active_substance_name
    , count
    , RANK() OVER (PARTITION BY serious_type ORDER BY count DESC) AS rnk
  FROM `ade-pipeline.ade_marts.drug_events_elderly`
),
drug_ranks as (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY serious_type ORDER BY count DESC) as row_num
    , serious_type
    , active_substance_name
    , count
  FROM 
    serious_drug_count
  WHERE 
    rnk <= 3
)
SELECT
  serious_type
  , active_substance_name
  , count
FROM
  drug_ranks
WHERE
  row_num <= 3
ORDER BY
  3 DESC