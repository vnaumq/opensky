-- Инициализация ClickHouse базы данных

-- Создание базы данных
CREATE DATABASE IF NOT EXISTS opensky;

USE opensky;

-- Таблица для потоковых данных (Streaming)
CREATE TABLE IF NOT EXISTS flights_streaming (
    icao24 String,
    callsign String,
    origin_country String,
    time_position UInt64,
    last_contact UInt64,
    longitude Float64,
    latitude Float64,
    baro_altitude Float64,
    on_ground UInt8,
    velocity Float64,
    true_track Float64,
    vertical_rate Float64,
    geo_altitude Float64,
    squawk String,
    spi UInt8,
    position_source UInt8,
    fetch_time DateTime,
    processing_time DateTime,
    is_valid_position UInt8,
    altitude_category String,
    speed_category String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(fetch_time)
ORDER BY (fetch_time, icao24)
TTL fetch_time + INTERVAL 30 DAY;

-- Таблица для batch данных (Batch)
CREATE TABLE IF NOT EXISTS flights_batch (
    icao24 String,
    callsign String,
    origin_country String,
    time_position UInt64,
    last_contact UInt64,
    longitude Float64,
    latitude Float64,
    baro_altitude Float64,
    on_ground UInt8,
    velocity Float64,
    true_track Float64,
    vertical_rate Float64,
    geo_altitude Float64,
    squawk String,
    spi UInt8,
    position_source UInt8,
    fetch_time DateTime,
    processing_time DateTime,
    is_valid_position UInt8,
    altitude_category String,
    speed_category String,
    batch_id String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(fetch_time)
ORDER BY (fetch_time, icao24)
TTL fetch_time + INTERVAL 90 DAY;

-- Материализованное представление для агрегации по странам
CREATE MATERIALIZED VIEW IF NOT EXISTS country_statistics_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(processing_time)
ORDER BY (origin_country, toStartOfHour(processing_time))
AS SELECT
    origin_country,
    toStartOfHour(processing_time) as hour,
    count() as flight_count,
    countDistinct(icao24) as unique_aircraft,
    avg(velocity) as avg_velocity,
    avg(baro_altitude) as avg_altitude,
    max(velocity) as max_velocity,
    min(velocity) as min_velocity,
    sum(on_ground) as grounded_count,
    sum(1 - on_ground) as airborne_count
FROM flights_streaming
GROUP BY origin_country, hour;

-- Таблица для статистики из S3 (для batch обработки)
CREATE TABLE IF NOT EXISTS flights_from_s3 (
    icao24 String,
    callsign String,
    origin_country String,
    time_position UInt64,
    last_contact UInt64,
    longitude Float64,
    latitude Float64,
    baro_altitude Float64,
    on_ground UInt8,
    velocity Float64,
    true_track Float64,
    vertical_rate Float64,
    geo_altitude Float64,
    squawk String,
    spi UInt8,
    position_source UInt8,
    fetch_time DateTime,
    processing_time DateTime,
    is_valid_position UInt8,
    altitude_category String,
    speed_category String,
    s3_path String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(fetch_time)
ORDER BY (fetch_time, icao24);

