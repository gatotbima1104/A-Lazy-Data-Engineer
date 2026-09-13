with enriched_detection as (
    select * from {{ ref('int_detection_enrich') }}
),

business_detection as (
    select

        detection_id,
        confidence_level_label,
        detection_period,
        detection_type_label,
        satellite_label,
        collection_version,
        processing_type

    from {{ ref('int_detection_business') }}
)

select

    ed.*,

    bd.confidence_level_label,
    bd.detection_period,
    bd.detection_type_label,
    bd.satellite_label,
    bd.collection_version,
    bd.processing_type

from enriched_detection ed

left join business_detection bd
    on ed.detection_id = bd.detection_id