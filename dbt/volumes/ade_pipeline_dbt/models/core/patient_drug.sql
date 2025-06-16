{{
  config(
    materialized = 'table',
    partition_by={"field" : "record_date", "data_type": "date"}
    )
}}

SELECT
    p.patientid
    , p.record_year
    , p.age_group
    , p.sex
    , p.weight_kgs
    , p.expedited_process
    , p.reporter_origin
    , p.report_origin
    , p.report_type
    , p.fda_received_date
    , p.fda_last_updated 
    , p.reported_date
    , p.age
    , p.serious_type
    , d.action_taken
    , d.drug_characterization
    , d.medicinal_product
    , d.active_substance_name
    , d.drug_indication
    , d.administration_route
    , d.drug_start_date
    , d.drug_end_date
    , d.drug_dosage_text
    , d.dosage_mg
    , d.treatment_duration_days
    , d.drug_reaction_after_readministration
    , DATE_TRUNC(p.fda_last_updated, YEAR) as record_date
from 
    {{ ref('stg_patient') }} p
    JOIN {{ ref('stg_drug') }} d ON p.patientid = d.patientid