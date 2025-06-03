select
    patientid
    , SAFE_CAST(recordyear as INT64) as record_year
    , {{ clean_text('reactionmeddrapt') }} as patient_reaction
    , SPLIT(reactionoutcome,"/")[0] AS reaction_outcome
from {{ source('external', 'ext_reaction') }}