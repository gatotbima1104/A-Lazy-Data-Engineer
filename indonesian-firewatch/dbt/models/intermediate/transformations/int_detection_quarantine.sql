with detections as (
    select * 
    from {{ ref('stg_detection') }}

    {% if is_incremental() %}
        {% if var('fire_detection_date', none) %}

            where acquisition_date = cast('{{ var("fire_detection_date") }}' as date)
            
        {% endif %}
    {% endif %}
)

select
    *,

    'REGION_NOT_MATCHED' as quarantine_issue,
    current_timestamp() as quarantined_at

from detections
where regency_id = -999