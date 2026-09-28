-- Monthly weather summary per city.
select
    city,
    country,
    date_trunc('month', weather_date)::date as month,
    count(*) as days_observed,
    round(avg(temp_mean_c), 1) as avg_temp_c,
    max(temp_max_c) as max_temp_c,
    min(temp_min_c) as min_temp_c,
    round(sum(precipitation_mm), 1) as total_precipitation_mm,
    count_if(is_rainy_day) as rainy_days
from {{ ref('fct_weather_daily') }}
group by all
