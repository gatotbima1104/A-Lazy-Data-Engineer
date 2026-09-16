{{
    config(
        materialized='incremental',
        unique_key='regency_province_daily_key',
        partition_by={
            'field': 'acquisition_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=[
            'regency_id'
        ]
    )
}}

with fact as (

    select *
    from {{ ref('fct_detection') }}

    {% if is_incremental() %}
        where acquisition_date = cast('{{ var("fire_detection_date") }}' as date)
    {% endif %}

),

location as (

    select
        regency_id,
        regency_name,
        province_id,
        province_name

    from {{ ref('dim_location') }}

),

satellite as (

    select
        satellite_id,
        satellite,
        satellite_label,
        instrument

    from {{ ref('dim_satellite') }}

),

classification as (

    select
        classification_id,
        confidence_level_label,
        daynight,
        detection_period,
        detection_type,
        detection_type_label

    from {{ ref('dim_detection_classification') }}

),

collecting_process as (

    select
        processing_id,
        version,
        collection_version,
        processing_type

    from {{ ref('dim_collecting_process') }}

)

select

    -- Mart key
    {{ 
        dbt_utils.generate_surrogate_key([
            'f.acquisition_date',
            'l.province_id',
            'l.regency_id',
            's.satellite_id',
            'c.classification_id',
            'cp.processing_id'
        ])
    }} as regency_province_daily_key,

    -- Date
    f.acquisition_date,
    f.ingestion_source,

    -- Geography
    l.province_id,
    l.province_name,
    l.regency_id,
    l.regency_name,

    -- Satellite
    s.satellite,
    s.satellite_label,
    s.instrument,

    -- Detection classification
    c.confidence_level_label,
    c.daynight,
    c.detection_period,
    c.detection_type,
    c.detection_type_label,

    -- Collecting process
    cp.version,
    cp.collection_version,
    cp.processing_type,

    -- Metrics
    count(*) as fire_detection_count,

    countif(c.confidence_level_label = 'high') as high_confidence_count,

    round(sum(f.fire_radiative_power), 2) as total_fire_radiative_power,

    round(avg(f.fire_radiative_power), 2) as avg_fire_radiative_power,

    round(avg(f.brightness), 2) as avg_brightness

from fact f

left join location l
    on f.regency_id = l.regency_id

left join satellite s
    on f.satellite_id = s.satellite_id

left join classification c
    on f.classification_id = c.classification_id

left join collecting_process cp
    on f.processing_id = cp.processing_id

group by
    regency_province_daily_key,
    f.acquisition_date,
    f.ingestion_source,
    l.province_id,
    l.province_name,
    l.regency_id,
    l.regency_name,
    s.satellite,
    s.satellite_label,
    s.instrument,
    c.confidence_level_label,
    c.daynight,
    c.detection_period,
    c.detection_type,
    c.detection_type_label,
    cp.version,
    cp.collection_version,
    cp.processing_type