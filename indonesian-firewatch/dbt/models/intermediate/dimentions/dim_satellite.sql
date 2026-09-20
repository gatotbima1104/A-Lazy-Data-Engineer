select distinct

    {{ 
        dbt_utils.generate_surrogate_key([
            'satellite',
            'instrument'
        ])
    }} as satellite_id,

    satellite,
    satellite_label,
    instrument

from {{ ref('int_detection_join') }}