INSERT INTO `famous-mix-471512-i3.dataset_weather_traffic.traffic_observations`
SELECT
  CURRENT_TIMESTAMP() AS observation_time,  -- หรือ map จากฟิลด์เวลาใน payload ถ้ามี
  routes[OFFSET(0)].legs[OFFSET(0)].start_address AS origin,
  routes[OFFSET(0)].legs[OFFSET(0)].end_address AS destination,
  SAFE_CAST(routes[OFFSET(0)].legs[OFFSET(0)].duration.value AS INT64) AS travel_time_seconds,
  SAFE_CAST(routes[OFFSET(0)].legs[OFFSET(0)].distance.value AS INT64) AS distance_meters,
  CASE
    WHEN routes[OFFSET(0)].legs[OFFSET(0)].duration_in_traffic.value IS NULL THEN 'unknown'
    WHEN routes[OFFSET(0)].legs[OFFSET(0)].duration_in_traffic.value >
         routes[OFFSET(0)].legs[OFFSET(0)].duration.value * 1.5 THEN 'heavy'
    WHEN routes[OFFSET(0)].legs[OFFSET(0)].duration_in_traffic.value >
         routes[OFFSET(0)].legs[OFFSET(0)].duration.value * 1.2 THEN 'moderate'
    ELSE 'light'
  END AS traffic_status,
  routes[OFFSET(0)].overview_polyline.points AS route_polyline,
  CURRENT_TIMESTAMP() AS load_time
FROM `famous-mix-471512-i3.dataset_weather_traffic.stg_traffic_raw`
WHERE DATE(_PARTITIONTIME) = CURRENT_DATE();
