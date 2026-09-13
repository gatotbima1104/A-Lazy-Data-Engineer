select
    acquisition_date,
    detection_hour,
    province_id,
    province_name,
    regency_id,
    regency_name,

    count(*) as fire_detection_count,

    round(sum(fire_radiative_power), 2) as total_detected_frp,
    round(avg(fire_radiative_power), 2) as avg_frp,
    round(max(fire_radiative_power), 2) as max_frp

from {{ ref('fct_detection') }}

group by
    acquisition_date,
    detection_hour,
    province_id,
    province_name,
    regency_id,
    regency_name