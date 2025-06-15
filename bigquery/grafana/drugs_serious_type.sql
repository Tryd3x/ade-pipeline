-- Top 3 drugs that associated with serious events in older adults
WITH ranked_drugs AS (
  SELECT 
    serious_type,
    active_substance_name,
    count,
    RANK() OVER (PARTITION BY serious_type ORDER BY count DESC) AS rnk,
    ROW_NUMBER() OVER (PARTITION BY serious_type ORDER BY count DESC) AS row_num
  FROM `ade-pipeline.ade_marts.drug_events_elderly`
)
SELECT
  serious_type,
  active_substance_name,
  count
FROM
  ranked_drugs
WHERE
  rnk <= 3 AND row_num <= 3
ORDER BY
  count DESC;
