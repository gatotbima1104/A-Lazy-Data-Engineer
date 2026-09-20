{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'acquisition_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['province_name']
    )
}}

with fact as (

    select *
    from {{ ref('fct_detection') }}

    {% if is_incremental() %}
        {% if var('fire_detection_date', none) %}
            where acquisition_date = cast('{{ var("fire_detection_date") }}' as date)
        {% else %}
            where acquisition_date >= date_sub(current_date(), interval 3 day)
        {% endif %}
    {% endif %}

),

classification as (

    select
        classification_id,
        confidence_level_label,
        daynight,
        detection_type_label
    from {{ ref('dim_detection_classification') }}

),

location as (

    select
        regency_id,
        regency_name,
        province_id,
        province_name

    from {{ ref('dim_location') }}

)



select

    f.acquisition_date,

    l.province_name,
    l.regency_name,

    case c.confidence_level_label
        when 'HIGH' then 'High'
        when 'NOMINAL' then 'Nominal'
        when 'LOW' then 'Low'
        else 'Unknown'
    end as confidence_level,

    case c.daynight
        when 'D' then 'Day'
        when 'N' then 'Night'
        else 'Unknown'
    end as day_night,

    case c.detection_type_label
        when 'VEGETATION_FIRE' then 'Vegetation Fire'
        when 'ACTIVE_VOLCANO' then 'Active Volcano'
        when 'STATIC_LAND_SOURCE' then 'Static Land Source'
        when 'OFFSHORE' then 'Offshore'
        else 'Unknown'
    end as detection_type_label,

    f.ingestion_source,

    -- Measures: hanya simpan yang bisa di-SUM atau di-MAX
    count(*) as detection_count,
    sum(f.fire_radiative_power) as total_frp,
    max(f.fire_radiative_power) as max_frp,
    sum(f.brightness) as total_brightness,
    max(f.brightness) as max_brightness

from fact f

left join classification c
    on f.classification_id = c.classification_id

left join location l
    on f.regency_id = l.regency_id

group by 1, 2, 3, 4, 5, 6, 7