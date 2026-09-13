{{
    config(
        materialized='incremental',
        unique_key='regency_daily_key',
        partition_by={
            'field': 'acquisition_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['province_id', 'regency_id']
    )
}}

select
    concat(
        cast(acquisition_date as string),
        '-',
        cast(regency_id as string)
    ) as regency_daily_key,

    acquisition_date,
    province_id,
    province_name,
    regency_id,
    regency_name,

    count(*) as fire_detection_count,

    countif(confidence_level_label = 'high') as high_confidence_count,
    countif(confidence_level_label = 'nominal') as nominal_confidence_count,
    countif(confidence_level_label = 'low') as low_confidence_count,

    sum(fire_radiative_power) as total_detected_frp,
    avg(fire_radiative_power) as avg_frp,
    max(fire_radiative_power) as max_frp,

    countif(daynight = 'D') as daytime_detection_count,
    countif(daynight = 'N') as nighttime_detection_count,

    count(distinct satellite) as satellite_count

from {{ ref('fct_detection') }}

{% if is_incremental() %}
where acquisition_date = cast('{{ var("fire_detection_date") }}' as date)
{% endif %}

group by
    acquisition_date,
    province_id,
    province_name,
    regency_id,
    regency_name