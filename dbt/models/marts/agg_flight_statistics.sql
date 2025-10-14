-- Агрегированная статистика по полетам
with hourly_stats as (
    select 
        date_trunc('hour', fetch_time) as hour_bucket,
        geographic_zone,
        origin_country,
        count(*) as total_flights,
        count(distinct icao24) as unique_aircraft,
        count(distinct callsign) as unique_callsigns,
        avg(velocity) as avg_velocity,
        avg(baro_altitude) as avg_altitude,
        max(velocity) as max_velocity,
        min(velocity) as min_velocity,
        count(case when on_ground = true then 1 end) as grounded_count,
        count(case when on_ground = false then 1 end) as airborne_count,
        count(case when altitude_class = 'low_altitude' then 1 end) as low_altitude_count,
        count(case when altitude_class = 'medium_altitude' then 1 end) as medium_altitude_count,
        count(case when altitude_class = 'high_altitude' then 1 end) as high_altitude_count,
        count(case when altitude_class = 'very_high_altitude' then 1 end) as very_high_altitude_count
    from {{ ref('fact_flight_activity') }}
    group by 
        date_trunc('hour', fetch_time),
        geographic_zone,
        origin_country
),

daily_stats as (
    select 
        date_trunc('day', hour_bucket) as day_bucket,
        geographic_zone,
        sum(total_flights) as daily_flights,
        sum(unique_aircraft) as daily_unique_aircraft,
        avg(avg_velocity) as daily_avg_velocity,
        avg(avg_altitude) as daily_avg_altitude,
        max(max_velocity) as daily_max_velocity,
        sum(grounded_count) as daily_grounded,
        sum(airborne_count) as daily_airborne,
        sum(low_altitude_count) as daily_low_altitude,
        sum(medium_altitude_count) as daily_medium_altitude,
        sum(high_altitude_count) as daily_high_altitude,
        sum(very_high_altitude_count) as daily_very_high_altitude
    from hourly_stats
    group by 
        date_trunc('day', hour_bucket),
        geographic_zone
)

select 
    day_bucket,
    geographic_zone,
    daily_flights,
    daily_unique_aircraft,
    round(daily_avg_velocity::numeric, 2) as daily_avg_velocity,
    round(daily_avg_altitude::numeric, 2) as daily_avg_altitude,
    daily_max_velocity,
    daily_grounded,
    daily_airborne,
    daily_low_altitude,
    daily_medium_altitude,
    daily_high_altitude,
    daily_very_high_altitude,
    round((daily_airborne::numeric / nullif(daily_flights, 0)) * 100, 2) as airborne_percentage,
    round((daily_high_altitude::numeric / nullif(daily_flights, 0)) * 100, 2) as high_altitude_percentage
from daily_stats
order by day_bucket desc, geographic_zone
