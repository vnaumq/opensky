-- Размерность стран
with country_stats as (
    select 
        origin_country,
        count(*) as total_flights,
        count(distinct icao24) as unique_aircraft,
        avg(velocity) as avg_velocity,
        avg(baro_altitude) as avg_altitude,
        max(velocity) as max_velocity,
        min(velocity) as min_velocity,
        count(case when on_ground = true then 1 end) as grounded_flights,
        count(case when on_ground = false then 1 end) as airborne_flights,
        min(fetch_time) as first_seen,
        max(fetch_time) as last_seen
    from {{ ref('stg_flights') }}
    group by origin_country
),

country_rankings as (
    select 
        *,
        row_number() over (order by total_flights desc) as flight_rank,
        row_number() over (order by unique_aircraft desc) as aircraft_rank,
        row_number() over (order by avg_velocity desc) as speed_rank
    from country_stats
)

select 
    origin_country,
    total_flights,
    unique_aircraft,
    round(avg_velocity::numeric, 2) as avg_velocity,
    round(avg_altitude::numeric, 2) as avg_altitude,
    max_velocity,
    min_velocity,
    grounded_flights,
    airborne_flights,
    first_seen,
    last_seen,
    flight_rank,
    aircraft_rank,
    speed_rank,
    case 
        when flight_rank <= 10 then 'top_10_countries'
        when flight_rank <= 50 then 'top_50_countries'
        else 'other_countries'
    end as country_tier
from country_rankings
