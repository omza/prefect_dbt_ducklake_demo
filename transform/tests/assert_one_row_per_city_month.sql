select city, month, count(*) as n
from {{ ref('agg_city_weather_monthly') }}
group by all
having count(*) > 1
