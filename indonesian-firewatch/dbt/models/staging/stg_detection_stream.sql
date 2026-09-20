with fire_detection_stream as (

    select

        event_id as detection_id,

        coalesce(cast(latitude as numeric), -999) as latitude,
        coalesce(cast(longitude as numeric), -999) as longitude,
        coalesce(cast(brightness as numeric), -999) as brightness,
        coalesce(cast(scan as numeric), -999) as scan,
        coalesce(cast(track as numeric), -999) as track,

        acquisition_date,
        acquisition_time,

        coalesce(cast(satellite as string), 'UNKNOWN') as satellite,
        coalesce(cast(instrument as string), 'UNKNOWN') as instrument,
        coalesce(cast(confidence as string), 'UNKNOWN') as confidence,

        coalesce(cast(version as string), 'UNKNOWN') as version,
        coalesce(cast(brightness_temperature_t31 as numeric), -999) as brightness_temperature_t31,
        coalesce(cast(fire_radiative_power as numeric), -999) as fire_radiative_power,
        
        coalesce(cast(daynight as string), 'UNKNOWN') as daynight,

        cast(null as numeric) as detection_type,
        coalesce(cast(regency_id as numeric), -999) as regency_id,

        'stream' as ingestion_source

    from {{ source('streaming', 'fire_detection_nrt_enriched') }}
    
)

select * from fire_detection_stream