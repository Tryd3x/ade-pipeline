-- Drugs Associated with reactions after readministration in Older Adults

WITH patient_readministered AS (
  SELECT
    serious_type,
    active_substance_name,
    COUNT(*) AS count
  FROM `ade-pipeline.ade_core.patient_drug_reaction`
  WHERE
    age_group = 'Elderly'
    AND active_substance_name NOT IN ('unknown', 'unspecified ingredient')
    AND serious_type <> 'Not Serious'
    AND drug_reaction_after_readministration = 'Yes'
  GROUP BY
    serious_type, active_substance_name
),
top_3_per_serious_type AS (
  SELECT
    serious_type,
    active_substance_name,
    count,
    ROW_NUMBER() OVER (PARTITION BY serious_type ORDER BY count DESC) AS row_num
  FROM
    patient_readministered
)
SELECT
  serious_type,
  active_substance_name,
  count
FROM
  top_3_per_serious_type
WHERE
  row_num <= 3
ORDER BY
  count DESC;

