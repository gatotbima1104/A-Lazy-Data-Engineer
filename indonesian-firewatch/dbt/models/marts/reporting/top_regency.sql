select 

    regency_name,
    count(*) as total_detection

from {{ ref('daily_detection') }}
group by regency_name
order by total_detection desc