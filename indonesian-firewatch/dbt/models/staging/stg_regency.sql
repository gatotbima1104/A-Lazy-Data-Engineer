with source_regency as (
    
    select
    
        coalesce(cast(regency_id as numeric), -999) as regency_id,
        coalesce(cast(province_id as numeric), -999) as province_id,
        coalesce(cast(regency_name as string), 'UNKNOWN') as regency_name

    from {{ source('detection', 'raw_regencies') }}
    
)

select * from source_regency