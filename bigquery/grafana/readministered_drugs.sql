-- Drugs Associated with reactions after readministration in Older Adults

WITH 
patient_readministered AS (
  SELECT 
    active_substance_name
    , count(*) as count
  FROM `ade-pipeline.ade_core.patient_drug_reaction`
  WHERE
      age_group = 'Elderly'
      and (active_substance_name <> 'unknown' and active_substance_name <> 'unspecified ingredient')
      and drug_reaction_after_readministration = 'Yes'
  GROUP BY 
    1
)
SELECT 
  active_substance_name
  , count
FROM 
  patient_readministered