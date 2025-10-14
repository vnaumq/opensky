-- Фактовая таблица активности полетов
with flight_activity as (
    select 
        icao24,
        callsign,
        origin_country,
        longitude,
        latitude,
        baro_altitude,
        velocity,
        true_track,
        vertical_rate,
        on_ground,
        altitude_category,
        speed_category,
        altitude_class,
        speed_class,
        position_timestamp,
        contact_timestamp,
        fetch_time,
        processing_time,
        distance_from_europe_center,
        
        -- Добавляем географические зоны
        case 
            when longitude between -10 and 40 and latitude between 35 and 70 then 'Europe'
            when longitude between -125 and -65 and latitude between 25 and 50 then 'North_America'
            when longitude between 100 and 180 and latitude between 10 and 60 then 'Asia_Pacific'
            else 'Other'
        end as geographic_zone,
        
        -- Добавляем временные зоны
        case 
            when extract(hour from position_timestamp) between 6 and 12 then 'morning'
            when extract(hour from position_timestamp) between 12 and 18 then 'afternoon'
            when extract(hour from position_timestamp) between 18 and 24 then 'evening'
            else 'night'
        end as time_of_day,
        
        -- Вычисляем активность
        case 
            when velocity > 0 and on_ground = false then 'active_flight'
            when velocity = 0 and on_ground = true then 'grounded'
            else 'unknown'
        end as flight_status

    from {{ ref('stg_flights') }}
)

select * from flight_activity
