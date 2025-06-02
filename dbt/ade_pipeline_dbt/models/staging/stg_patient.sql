 
{# Rename Columns 
Round values
SafeCast #}

select
    patientid
    , SAFE_CAST(recordyear as INT64) as record_year
    , TRIM(age_group) as age_group
    , age_years as age
    , TRIM(sex) as sex
    , ROUND(weight,2) as weight_kgs
    , TRIM(expedited_process) as expedited_process
    , TRIM(primarysourcecountry) as reporter_origin
    , TRIM(occurcountry) as report_origin
    , TRIM(report_type) as report_type
    , receipt_date as fda_last_updated
    , receive_date as fda_received_date
    , TRIM(safetyreportid) as safety_report_id
    , transmission_date as reported_date
    , TRIM(serious_type) as serious_type
from {{ source('external', 'ext_patient') }}