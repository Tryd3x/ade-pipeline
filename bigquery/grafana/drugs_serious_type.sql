-- Top 5 drugs that causing serious events in older adults
WITH serious_drug_count AS ( 
  SELECT 
    serious_type
    , active_substance_name
    , count
    , RANK() OVER (PARTITION BY serious_type ORDER BY count DESC) AS rnk
  FROM `zoomcamp-454219.ade_dev_marts.drug_events_elderly`
)
SELECT
  serious_type
  , active_substance_name
  , count
FROM 
  serious_drug_count
WHERE 
  rnk <= 3
ORDER BY 
  count DESC