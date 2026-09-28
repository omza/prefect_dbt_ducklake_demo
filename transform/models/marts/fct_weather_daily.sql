-- Daily weather enriched with a readable weather description.
select
    w.weather_day_id,
    w.city,
    w.country,
    w.weather_date,
    w.temp_min_c,
    w.temp_max_c,
    w.temp_mean_c,
    w.temp_max_c - w.temp_min_c as temp_range_c,
    w.precipitation_mm,
    w.precipitation_mm > 1.0 as is_rainy_day,
    w.wind_speed_max_kmh,
    w.weather_code,
    coalesce(c.description, 'Unknown') as weather_description,
    coalesce(c.category, 'unknown') as weather_category
from {{ ref('stg_weather_daily') }} as w
left join {{ ref('weather_codes') }} as c
    on w.weather_code = c.weather_code
