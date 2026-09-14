select distinct

    {{ 
        dbt_utils.generate_surrogate_key([
            'version',
            'collection_version',
            'processing_type'
        ])
    }} as processing_id,

    version,
    collection_version,
    processing_type

from {{ ref('int_detection_join') }}