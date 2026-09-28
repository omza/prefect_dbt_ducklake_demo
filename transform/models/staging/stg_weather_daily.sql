-- Clean, rename and type the raw weather data. No business logic here.
select
    lower(city) || '_' || strftime(date, '%Y%m%d') as weather_day_id,
    city,
    country,
    date as weather_date,
    temperature_2m_min as temp_min_c,
    temperature_2m_max as temp_max_c,
    temperature_2m_mean as temp_mean_c,
    coalesce(precipitation_sum, 0) as precipitation_mm,
    wind_speed_10m_max as wind_speed_max_kmh,
    weather_code,
    _loaded_at as loaded_at
from {{ source('raw', 'weather_daily') }}
where temperature_2m_mean is not null
