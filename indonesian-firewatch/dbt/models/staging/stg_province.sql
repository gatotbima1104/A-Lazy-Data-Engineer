with source_province as (
    
    select

        coalesce(cast(province_id as numeric), -999) as province_id,
        coalesce(cast(province_name as string), 'UNKNOWN') as province_name

    from {{ source('detection', 'raw_provinces') }}
    
)

select * from source_province