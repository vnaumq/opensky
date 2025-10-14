-- Модель для очистки и нормализации данных о полетах
with source_data as (
    select * from {{ source('raw', 'processed_flights') }}
),

cleaned_flights as (
    select
        icao24,
        callsign,
        origin_country,
        time_position,
        last_contact,
        longitude,
        latitude,
        baro_altitude,
        on_ground,
        velocity,
        true_track,
        vertical_rate,
        sensors,
        geo_altitude,
        squawk,
        spi,
        position_source,
        fetch_time,
        processing_time,
        is_valid_position,
        altitude_category,
        speed_category,
        
        -- Добавляем временные метки
        case 
            when time_position is not null 
            then to_timestamp(time_position) 
            else null 
        end as position_timestamp,
        
        case 
            when last_contact is not null 
            then to_timestamp(last_contact) 
            else null 
        end as contact_timestamp,
        
        -- Вычисляем расстояние от центра Европы (примерно)
        sqrt(power(longitude - 10.0, 2) + power(latitude - 50.0, 2)) as distance_from_europe_center,
        
        -- Категоризируем высоту
        case 
            when baro_altitude < 1000 then 'low_altitude'
            when baro_altitude < 10000 then 'medium_altitude'
            when baro_altitude < 20000 then 'high_altitude'
            else 'very_high_altitude'
        end as altitude_class,
        
        -- Категоризируем скорость
        case 
            when velocity < 100 then 'slow'
            when velocity < 500 then 'medium'
            when velocity < 800 then 'fast'
            else 'very_fast'
        end as speed_class

    from source_data
    where 
        is_valid_position = true
        and longitude is not null 
        and latitude is not null
        and longitude between -180 and 180
        and latitude between -90 and 90
)

select * from cleaned_flights
