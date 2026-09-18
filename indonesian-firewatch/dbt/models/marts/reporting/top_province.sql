select 

    province_name,
    count(*) as total_detection

from {{ ref('daily_detection') }}
group by province_name
order by total_detection desc