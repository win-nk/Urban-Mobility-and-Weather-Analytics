INSERT INTO `famous-mix-471512-i3.dataset_weather_traffic.weather_observations`
SELECT
  TIMESTAMP_SECONDS(dt) AS observation_time,
  name AS city,
  sys.country AS country,
  main.temp AS temp_celsius,
  main.feels_like AS feels_like_celsius,
  CAST(main.humidity AS INT64) AS humidity,
  SAFE_CAST(wind.speed AS FLOAT64) AS wind_speed_m_s,
  weather[OFFSET(0)].main AS weather_main,
  weather[OFFSET(0)].description AS weather_description,
  CURRENT_TIMESTAMP() AS load_time
FROM `famous-mix-471512-i3.dataset_weather_traffic.stg_weather_raw`
WHERE DATE(_PARTITIONTIME) = CURRENT_DATE();
