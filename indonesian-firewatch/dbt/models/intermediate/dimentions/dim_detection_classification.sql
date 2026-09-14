select distinct

    {{
        dbt_utils.generate_surrogate_key([
            'confidence_level_label',
            'daynight',
            'detection_period',
            'detection_type',
            'detection_type_label'
        ])
    }} as classification_id,

    confidence_level_label,
    daynight,
    detection_period,
    detection_type,
    detection_type_label

from {{ ref('int_detection_join') }}