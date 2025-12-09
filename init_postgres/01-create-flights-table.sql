-- ./init_postgres/01-flights-table.sql
-- Этот файл выполняется ТОЛЬКО при первом создании базы (когда volume пустой)

\echo '=== Создаём таблицу flights_raw ==='

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

-- Индексы
CREATE INDEX IF NOT EXISTS idx_flights_raw_ingestion_time ON flights_raw (ingestion_time DESC);
CREATE INDEX IF NOT EXISTS idx_flights_raw_callsign       ON flights_raw (callsign);
CREATE INDEX IF NOT EXISTS idx_flights_raw_icao24         ON flights_raw (icao24);

\echo 'Таблица flights_raw успешно создана/проверена'