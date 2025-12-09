-- init-db/01-create-flights-table.sql
CREATE TABLE IF NOT EXISTS flights_raw (
    icao24            VARCHAR(10)     NOT NULL,
    callsign          VARCHAR(20),
    origin_country    VARCHAR(100),
    time_position     BIGINT,
    last_contact      BIGINT,
    longitude         DOUBLE PRECISION,
    latitude          DOUBLE PRECISION,
    baro_altitude     DOUBLE PRECISION,
    on_ground         BOOLEAN         NOT NULL DEFAULT FALSE,
    velocity          DOUBLE PRECISION,
    true_track        DOUBLE PRECISION,
    vertical_rate     DOUBLE PRECISION,
    sensors           INTEGER[],
    geo_altitude      DOUBLE PRECISION,
    squawk            VARCHAR(10),
    spi               BOOLEAN,
    position_source   SMALLINT,
    ingestion_time    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (icao24, time_position)
);

-- Полезные индексы
CREATE INDEX IF NOT EXISTS idx_flights_raw_ingestion_time ON flights_raw (ingestion_time DESC);
CREATE INDEX IF NOT EXISTS idx_flights_raw_callsign       ON flights_raw (callsign);
CREATE INDEX IF NOT EXISTS idx_flights_raw_icao24         ON flights_raw (icao24);
CREATE INDEX IF NOT EXISTS idx_flights_raw_country        ON flights_raw (origin_country);

-- -- Бонус: последние позиции каждого самолёта (для дашбордов — бесценно!)
-- CREATE MATERIALIZED VIEW IF NOT EXISTS flights_latest AS
-- SELECT DISTINCT ON (icao24) *
-- FROM flights_raw
-- ORDER BY icao24, ingestion_time DESC;

-- CREATE INDEX IF NOT EXISTS idx_flights_latest_icao24 ON flights_latest (icao24);

-- -- Авто-обновление вьюхи каждые 30 сек (опционально, через pg_cron)
-- -- Если у тебя установлен pg_cron, раскомментируй:
-- SELECT cron.schedule('refresh-flights-latest', '30 seconds', $$REFRESH MATERIALIZED VIEW flights_latest$$);