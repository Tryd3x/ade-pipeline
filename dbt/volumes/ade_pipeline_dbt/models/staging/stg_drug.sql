select 
    patientid
    , SAFE_CAST(recordyear as INT64) as record_year
    , TRIM(actiondrug) as action_taken
    , TRIM(drugcharacterization) as drug_characterization
    , {{ clean_text('medicinalproduct') }} as medicinal_product
    , {{ clean_text('activesubstancename') }} as active_substance_name
    , {{ clean_text('drug_indication') }} as drug_indication
    , TRIM(administration_route) as administration_route
    , drug_start_date
    , drug_end_date
    , {{ clean_text('drugdosagetext') }} as drug_dosage_text
    , ROUND(dosage_mg, 6) as dosage_mg
    , ROUND(treatment_duration_days,6) as treatment_duration_days
    , TRIM(drug_reaction_after_readministration) as drug_reaction_after_readministration
from {{ source('external', 'ext_drug') }}
