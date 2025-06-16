-- Serious Drug Events by Year & Type in Older Adults 
SELECT
  cast(record_year as string) as record_year
  , serious_type
  , count(*) as  cnt
FROM `ade-pipeline.ade_core.patient_drug_reaction`
WHERE
  age_group = "Elderly"
  AND serious_type <> "Not Serious"
GROUP BY
  1,2
ORDER BY
  1 desc, 2, 3 desc

{# SELECT
  CAST(record_year AS STRING) AS record_year,
  COUNTIF(serious_type = 'Death') AS Death,
  COUNTIF(serious_type = 'Hospitalization') AS Hospitalization,
  COUNTIF(serious_type = 'Disabling') AS Disabling,
  COUNTIF(serious_type = 'Congenitalanomali') AS Congenitalanomali,
  COUNTIF(serious_type = 'Lifethreatening') AS Lifethreatening,
  COUNTIF(serious_type = 'Other') AS Other
FROM `zoomcamp-454219.ade_dev_core.patient_drug_reaction`
WHERE
  age_group = "Elderly"
  AND serious_type <> "Not Serious"
GROUP BY
  record_year
ORDER BY
  record_year DESC #}


