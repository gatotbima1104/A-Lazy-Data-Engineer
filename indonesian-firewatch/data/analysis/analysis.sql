SELECT * FROM `jcdeah-009.fp_gatot_dataset_mart.agg_detection_daily`

-- Regency_Province mana yang paling banyak terdeteksi api
select
    regency_name,
    province_name,
    detection_type_label,
    sum(fire_detection_count) as total_fire_detections,
FROM `jcdeah-009.fp_gatot_dataset_mart.agg_detection_daily`
WHERE detection_type_label = 'STATIC_LAND_SOURCE'
  or detection_type_label = 'VEGETATION_FIRE'
group by 1, 2, 3
order by total_fire_detections desc

-- The most province with fire detection
select
  province_name,
  sum(fire_detection_count) as total_fire_detections,
  sum(total_fire_radiative_power) as total_frp
FROM `jcdeah-009.fp_gatot_dataset_mart.agg_detection_daily`
GROUP BY 1
ORDER BY 2 DESC

-- Seeing Tren Fire Each Day
select
    acquisition_date,
    sum(fire_detection_count) as fire_detections,
    sum(total_fire_radiative_power) as total_frp
FROM `jcdeah-009.fp_gatot_dataset_mart.agg_detection_daily`
group by 1
order by 1