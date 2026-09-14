{{
    config(
        materialized='incremental',
        unique_key='detection_id',
        partition_by={
            'field': 'acquisition_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=[
            'regency_id',
            'satellite_id'
        ]
    )
}}

with detection as (
    select * 
    from {{ ref('int_detection_join') }}

    {% if is_incremental() %}
        where acquisition_date = cast('{{ var("fire_detection_date") }}' as date)
    {% endif %}
)

select

    -- Detection keys
    detection_id,

    -- FK Keys
    regency_id,

    -- Satellite foreign key
    {{ dbt_utils.generate_surrogate_key([
        'satellite',
        'instrument'
    ]) }} as satellite_id,

    -- Detection classification foreign key
    {{ dbt_utils.generate_surrogate_key([
        'confidence_level_label',
        'daynight',
        'detection_period',
        'detection_type',
        'detection_type_label'
    ]) }} as classification_id,

    -- Collecting process foreign key
    {{ dbt_utils.generate_surrogate_key([
        'version',
        'collection_version',
        'processing_type'
    ]) }} as processing_id,

    -- Measures
    brightness,
    scan,
    track,
    confidence,
    brightness_temperature_t31,
    fire_radiative_power,

    -- Time Dimention
    acquisition_date,
    acquisition_time,
    detection_datetime,
    detection_timestamp

from detection