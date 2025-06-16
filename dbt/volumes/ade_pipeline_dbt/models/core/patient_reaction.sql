{{
  config(
    materialized = 'table',
    partition_by={"field" : "record_date", "data_type": "date"}
    )
}}

select
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
    , r.patient_reaction
    , r.reaction_outcome
    , DATE_TRUNC(p.fda_last_updated, YEAR) as record_date
from 
    {{ ref('stg_patient') }} p
    JOIN {{ ref('stg_reaction') }} r ON r.patientid = p.patientid 