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
            'province_id',
            'regency_id',
            'detection_type',
            'confidence_level_label'
        ]
    )
}}

with detection as (
    select * from {{ ref('int_detection_join') }}
),
regency as (
    select * from {{ ref('dim_regency') }}
),
province as (
    select * from {{ ref('dim_province') }}
)

select

    -- Detection id
    d.detection_id,

    -- Geographi
    d.latitude,
    d.longitude,

    -- Location Details
    d.regency_id,
    r.regency_name,

    r.province_id,
    p.province_name,

    concat(
        r.regency_name,
        ', ',
        p.province_name
    ) as location_detail,

    -- Detection attributes
    d.brightness,
    d.scan,
    d.track,

    -- Acquisition
    d.acquisition_date,
    d.acquisition_time,
    d.detection_datetime,
    d.detection_timestamp,
    d.detection_year,
    d.detection_month,
    d.detection_month_name,
    d.detection_day_of_week,
    d.detection_day_name,
    d.detection_hour,

    -- Satellite
    d.satellite,
    d.satellite_label,
    
    d.instrument,

    -- Detection characteristics

    -- Business Confindence classification
    d.confidence,
    d.confidence_level_label,

    -- Processing Version
    d.version,
    d.collection_version,
    d.processing_type,

    d.brightness_temperature_t31,
    d.fire_radiative_power,

    -- Business Period classification
    d.daynight,
    d.detection_period,
    
    -- Business Type classification
    d.detection_type,
    d.detection_type_label

from detection d

left join regency r
    on d.regency_id = r.regency_id

left join province p
    on r.province_id = p.province_id

-- Incremental Checking
{% if is_incremental() %}

where d.acquisition_date = cast('{{ var("fire_detection_date") }}' as date)

{% endif %}