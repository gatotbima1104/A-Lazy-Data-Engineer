with batch as (
    select * from {{ ref('stg_detection_batch') }}
),

stream as (
    select * from {{ ref('stg_detection_stream') }}
),

unioned as (
    
    select * from batch
    
    union all

    select * from stream

)

select *
from unioned