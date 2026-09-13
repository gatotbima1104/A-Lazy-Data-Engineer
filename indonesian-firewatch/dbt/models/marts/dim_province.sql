select 

    province_id, province_name 
    
from {{ ref('stg_province') }}