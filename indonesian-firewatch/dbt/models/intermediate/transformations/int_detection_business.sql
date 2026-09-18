with detection as (
    
    select * from {{ ref('stg_detection') }}
    where regency_id != -999

)
select
    *,
    
    case
        when lower(confidence) = 'h' then 'HIGH'
        when lower(confidence) = 'n' then 'NOMINAL'
        when lower(confidence) = 'l' then 'LOW'
        else 'UNKNOWN'
    end as confidence_level_label,

    case
        when upper(daynight) = 'D' then 'DAY'
        when upper(daynight) = 'N' then 'NIGHT'
        else 'UNKNOWN'
    end as detection_period,

    case
        when detection_type = 0 then 'VEGETATION_FIRE'
        when detection_type = 1 then 'ACTIVE_VOLCANO'
        when detection_type = 2 then 'STATIC_LAND_SOURCE'
        when detection_type = 3 then 'OFFSHORE'
        else 'UNKNOWN'
    end as detection_type_label,

    case
        when upper(satellite) = 'SNPP' then 'SUOMI NPP'
        when upper(satellite) = 'N20' then 'NOAA-20'
        when upper(satellite) = 'N21' then 'NOAA-21'
        else 'UNKNOWN'
    end as satellite_label,

    case
        -- Example: '2.0NRT' → '2.0'
            when regexp_contains(
                cast(version as string), 
                r'^[0-9]+(?:\.[0-9]+)?'
            ) then regexp_extract(
                    cast(version as string),
                    r'^([0-9]+(?:\.[0-9]+)?)'
                )
        else null
    end as collection_version,

    case
        -- Example: '2.0NRT' → 'NEAR_REAL_TIME'
        when upper(cast(version as string)) like '%NRT' then 'NEAR_REAL_TIME'

        -- Example: '2.0URT' → 'ULTRA_REAL_TIME'
        when upper(cast(version as string)) like '%URT' then 'ULTRA_REAL_TIME'

        -- Example: '2.0RT' → 'REAL_TIME'
        when upper(cast(version as string)) like '%RT' then 'REAL_TIME'

        -- Example: '2', '2.0' → 'STANDARD'
        when regexp_contains(
            cast(version as string), 
            r'^[0-9]+(?:\.[0-9]+)?$'
        ) then 'STANDARD'

        else 'UNKNOWN'
    end as processing_type


from detection