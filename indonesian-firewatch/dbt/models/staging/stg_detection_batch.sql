with fire_detection as (
    
    select
    
        to_hex(md5(
            concat(
                cast(latitude as string), '|',
                cast(longitude as string), '|',
                cast(acq_date as string), '|',
                cast(acq_time as string), '|',
                coalesce(satellite, ''), '|',
                coalesce(instrument, '')
            )
        )) as detection_id,

        coalesce(cast(latitude as numeric), -999) as latitude,
        coalesce(cast(longitude as numeric), -999) as longitude,
        coalesce(cast(brightness as numeric), -999) as brightness,
        coalesce(cast(scan as numeric), -999) as scan,
        coalesce(cast(track as numeric), -999) as track,

        cast(acq_date as date) as acquisition_date,

        cast(
            parse_time('%H%M', lpad(cast(acq_time as string), 4, '0'))
            as time
        ) as acquisition_time,

        coalesce(cast(satellite as string), 'UNKNOWN') as satellite,
        coalesce(cast(instrument as string), 'UNKNOWN') as instrument,
        coalesce(cast(confidence as string), 'UNKNOWN') as confidence,

        coalesce(cast(version as string), 'UNKNOWN') as version,
        coalesce(cast(bright_t31 as numeric), -999) as brightness_temperature_t31,
        coalesce(cast(frp as numeric), -999) as fire_radiative_power,

        coalesce(cast(daynight as string), 'UNKNOWN') as daynight,
        coalesce(cast(type as numeric), -999) as detection_type,
        coalesce(cast(regency_id as numeric), -999) as regency_id,

        'batch' as ingestion_source

    from {{ source('detection', 'raw_detections') }}

)

select * from fire_detection