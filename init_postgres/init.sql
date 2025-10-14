-- Инициализация базы данных OpenSky

-- Создание таблицы для сырых данных
CREATE TABLE IF NOT EXISTS processed_flights (
    id SERIAL PRIMARY KEY,
    icao24 VARCHAR(10),
    callsign VARCHAR(20),
    origin_country VARCHAR(10),
    time_position BIGINT,
    last_contact BIGINT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    on_ground BOOLEAN,
    velocity DOUBLE PRECISION,
    true_track DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    sensors TEXT,
    geo_altitude DOUBLE PRECISION,
    squawk VARCHAR(10),
    spi BOOLEAN,
    position_source INTEGER,
    fetch_time TIMESTAMP,
    processing_time TIMESTAMP,
    is_valid_position BOOLEAN,
    altitude_category VARCHAR(20),
    speed_category VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы для статистики
CREATE TABLE IF NOT EXISTS flight_statistics (
    id SERIAL PRIMARY KEY,
    origin_country VARCHAR(10),
    altitude_category VARCHAR(20),
    flight_count INTEGER,
    avg_velocity DOUBLE PRECISION,
    avg_altitude DOUBLE PRECISION,
    max_velocity DOUBLE PRECISION,
    min_velocity DOUBLE PRECISION,
    grounded_count INTEGER,
    airborne_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы для метрик
CREATE TABLE IF NOT EXISTS flight_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value DOUBLE PRECISION,
    metric_timestamp TIMESTAMP,
    additional_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы для мониторинга
CREATE TABLE IF NOT EXISTS monitoring_alerts (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50),
    alert_message TEXT,
    severity VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для оптимизации
CREATE INDEX IF NOT EXISTS idx_processed_flights_fetch_time ON processed_flights(fetch_time);
CREATE INDEX IF NOT EXISTS idx_processed_flights_icao24 ON processed_flights(icao24);
CREATE INDEX IF NOT EXISTS idx_processed_flights_origin_country ON processed_flights(origin_country);
CREATE INDEX IF NOT EXISTS idx_processed_flights_coordinates ON processed_flights(longitude, latitude);

CREATE INDEX IF NOT EXISTS idx_flight_statistics_created_at ON flight_statistics(created_at);
CREATE INDEX IF NOT EXISTS idx_flight_statistics_country ON flight_statistics(origin_country);

CREATE INDEX IF NOT EXISTS idx_flight_metrics_timestamp ON flight_metrics(metric_timestamp);
CREATE INDEX IF NOT EXISTS idx_flight_metrics_name ON flight_metrics(metric_name);

CREATE INDEX IF NOT EXISTS idx_monitoring_alerts_created_at ON monitoring_alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_monitoring_alerts_severity ON monitoring_alerts(severity);

-- Создание представлений для аналитики
CREATE OR REPLACE VIEW v_flight_summary AS
SELECT 
    DATE_TRUNC('hour', fetch_time) as hour_bucket,
    COUNT(*) as total_flights,
    COUNT(DISTINCT icao24) as unique_aircraft,
    COUNT(DISTINCT origin_country) as unique_countries,
    AVG(velocity) as avg_velocity,
    AVG(baro_altitude) as avg_altitude,
    COUNT(CASE WHEN on_ground = true THEN 1 END) as grounded_count,
    COUNT(CASE WHEN on_ground = false THEN 1 END) as airborne_count
FROM processed_flights
WHERE fetch_time >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', fetch_time)
ORDER BY hour_bucket DESC;

-- Создание представления для топ стран
CREATE OR REPLACE VIEW v_top_countries AS
SELECT 
    origin_country,
    COUNT(*) as flight_count,
    COUNT(DISTINCT icao24) as unique_aircraft,
    AVG(velocity) as avg_velocity,
    AVG(baro_altitude) as avg_altitude
FROM processed_flights
WHERE fetch_time >= NOW() - INTERVAL '24 hours'
GROUP BY origin_country
ORDER BY flight_count DESC
LIMIT 20;
