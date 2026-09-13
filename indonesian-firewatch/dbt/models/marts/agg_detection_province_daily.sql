{{
    config(
        materialized='incremental',
        unique_key='province_daily_key',
        partition_by={
            'field': 'acquisition_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['province_id']
    )
}}

select
    concat(
        cast(acquisition_date as string),
        '-',
        cast(province_id as string)
    ) as province_daily_key,

    acquisition_date,
    province_id,
    province_name,

    count(*) as fire_detection_count,

    countif(confidence_level_label = 'high') as high_confidence_count,
    countif(confidence_level_label = 'nominal') as nominal_confidence_count,
    countif(confidence_level_label = 'low') as low_confidence_count,

    round(sum(fire_radiative_power), 2) as total_detected_frp,
    round(avg(fire_radiative_power), 2) as avg_frp,
    round(max(fire_radiative_power), 2) as max_frp,

    countif(daynight = 'D') as daytime_detection_count,
    countif(daynight = 'N') as nighttime_detection_count,

    count(distinct regency_id) as affected_regency_count,
    count(distinct satellite) as satellite_count

from {{ ref('fct_detection') }}

{% if is_incremental() %}
where acquisition_date = cast(
    '{{ var("fire_detection_date") }}'
    as date
)
{% endif %}

group by
    acquisition_date,
    province_id,
    province_name