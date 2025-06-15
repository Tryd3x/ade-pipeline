-- Reports by Year
SELECT
  cast(record_year as string)
  ,count(patientid) as cnt
FROM 
  `ade-pipeline.ade_core.patient_drug_reaction`
GROUP BY
  1
ORDER BY
  1 DESC