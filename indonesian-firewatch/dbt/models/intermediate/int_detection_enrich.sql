with detection as (

    select * 
    from {{ ref('stg_detection') }}

),

enriched as (

    select
        *,

        datetime(
            acquisition_date, 
            acquisition_time
        ) as detection_datetime,

        timestamp(
            datetime(acquisition_date, acquisition_time)
        ) as detection_timestamp,

        extract(year from acquisition_date) as detection_year,
        extract(month from acquisition_date) as detection_month,
        extract(dayofweek from acquisition_date) as detection_day_of_week,
        extract(hour from acquisition_time) as detection_hour

    from detection

)

select 
    *,

    case
        when detection_month = 1 then 'JANUARY'
        when detection_month = 2 then 'FEBRUARY'
        when detection_month = 3 then 'MARCH'
        when detection_month = 4 then 'APRIL'
        when detection_month = 5 then 'MAY'
        when detection_month = 6 then 'JUNE'
        when detection_month = 7 then 'JULY'
        when detection_month = 8 then 'AUGUST'
        when detection_month = 9 then 'SEPTEMBER'
        when detection_month = 10 then 'OCTOBER'
        when detection_month = 11 then 'NOVEMBER'
        when detection_month = 12 then 'DECEMBER'
        else 'UNKNOWN'
    end as detection_month_name,

    case
        when detection_day_of_week = 1 then 'SUNDAY'
        when detection_day_of_week = 2 then 'MONDAY'
        when detection_day_of_week = 3 then 'TUESDAY'
        when detection_day_of_week = 4 then 'WEDNESDAY'
        when detection_day_of_week = 5 then 'THURSDAY'
        when detection_day_of_week = 6 then 'FRIDAY'
        when detection_day_of_week = 7 then 'SATURDAY'
        else 'UNKNOWN'
    end as detection_day_name

from enriched 