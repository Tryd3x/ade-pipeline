select
    serious_type
    , active_substance_name
    , COUNT(*) as count
from 
    {{ ref('patient_drug_reaction') }}
where
    age_group = 'Elderly'
    and active_substance_name not in ('unknown', 'unspecified ingredient')
    and serious_type <> 'Not Serious'
group by 1,2
order by 3 desc,1 desc