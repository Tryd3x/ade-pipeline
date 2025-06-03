-- Most frequent Drugs Associated with Serious Event Type in Older Adults

SELECT
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
  record_year DESC
