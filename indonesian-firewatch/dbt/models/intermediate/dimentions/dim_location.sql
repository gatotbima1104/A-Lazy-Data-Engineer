select

    r.regency_id, 
    r.regency_name,
    r.province_id,
    p.province_name
    

from {{ ref('stg_regency') }} r

left join {{ ref('stg_province') }} p
    on p.province_id = r.province_id