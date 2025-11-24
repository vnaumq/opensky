"""
Конфигурация проекта OpenSky
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Настройки приложения"""

    # OpenSky API
    opensky_client_id: str = Field(..., env="OPENSKY_CLIENT_ID")
    opensky_client_secret: str = Field(..., env="OPENSKY_CLIENT_SECRET")

    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_raw_flights: str = Field(default="raw_flights", env="KAFKA_TOPIC_RAW_FLIGHTS")
    kafka_topic_processed_flights: str = Field(default="processed_flights", env="KAFKA_TOPIC_PROCESSED_FLIGHTS")

    # Database
    postgres_host: str = Field(default="localhost", env="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, env="POSTGRES_PORT")
    postgres_db: str = Field(default="opensky", env="POSTGRES_DB")
    postgres_user: str = Field(default="opensky_user", env="POSTGRES_USER")
    postgres_password: str = Field(..., env="POSTGRES_PASSWORD")

    # Spark
    spark_master: str = Field(default="local[*]", env="SPARK_MASTER")
    spark_app_name: str = Field(default="opensky_processor", env="SPARK_APP_NAME")

    # ClickHouse
    clickhouse_host: str = Field(default="localhost", env="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=8123, env="CLICKHOUSE_PORT")
    clickhouse_db: str = Field(default="opensky", env="CLICKHOUSE_DB")
    clickhouse_user: str = Field(default="default", env="CLICKHOUSE_USER")
    clickhouse_password: str = Field(default="clickhouse_password", env="CLICKHOUSE_PASSWORD")

    # MinIO / S3
    minio_endpoint: str = Field(default="localhost:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin123", env="MINIO_SECRET_KEY")
    minio_bucket_raw: str = Field(default="raw-data", env="MINIO_BUCKET_RAW")
    minio_bucket_processed: str = Field(default="processed-data", env="MINIO_BUCKET_PROCESSED")

    # Airflow
    airflow_home: str = Field(default="/opt/airflow", env="AIRFLOW_HOME")
    airflow_executor: str = Field(default="LocalExecutor", env="AIRFLOW__CORE__EXECUTOR")

    # Monitoring
    prometheus_port: int = Field(default=9090, env="PROMETHEUS_PORT")
    grafana_port: int = Field(default=3000, env="GRAFANA_PORT")

    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    @property
    def database_url(self) -> str:
        """URL подключения к базе данных"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def airflow_sql_alchemy_conn(self) -> str:
        """URL подключения к базе данных для Airflow"""
        return self.database_url

    class Config:
        env_file = ".env"
        case_sensitive = False


# Глобальный экземпляр настроек
settings = Settings()
