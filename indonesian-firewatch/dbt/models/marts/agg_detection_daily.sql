select
    acquisition_date,
    province_id,
    province_name,
    regency_id,
    regency_name,

    count(*) as fire_detection_count,

    countif(confidence_level_label = 'high') as high_confidence_count,

    round(sum(fire_radiative_power), 2) as total_fire_radiative_power,
    round(avg(fire_radiative_power), 2) as avg_fire_radiative_power,
    round(max(fire_radiative_power), 2) as max_fire_radiative_power,
    
    countif(daynight = 'D') as daytime_detection_count,
    countif(daynight = 'N') as nighttime_detection_count

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