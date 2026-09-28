-- A "singular" test: any row returned counts as a failure.
select *
from {{ ref('stg_weather_daily') }}
where temp_min_c > temp_max_c
