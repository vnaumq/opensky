"""
Spark Batch Job для обработки исторических данных о полетах
Демонстрирует различия между batch и streaming обработкой
"""
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, isnan, isnull,
    lit, current_timestamp, count, countDistinct, avg, max as spark_max,
    min as spark_min, collect_list, struct, date_format,
    year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

from config.settings import settings
from config.logging import get_logger

logger = get_logger(__name__)


class BatchFlightProcessor:
    """Обработчик batch данных о полетах"""

    def __init__(self):
        self.spark = None
        self._setup_spark()

    def _setup_spark(self) -> None:
        """Настройка Spark Session для batch обработки"""
        try:
            self.spark = SparkSession.builder \
                .appName("opensky_batch_processor") \
                .master(settings.spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.warehouse.dir", "/opt/bitnami/spark/data/warehouse") \
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark Batch Session создан успешно")

        except Exception as e:
            logger.error(f"Ошибка создания Spark Session: {e}")
            raise

    def _get_flight_schema(self) -> StructType:
        """Схема данных о полетах"""
        return StructType([
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", IntegerType(), True),
            StructField("last_contact", IntegerType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("baro_altitude", DoubleType(), True),
            StructField("on_ground", BooleanType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("true_track", DoubleType(), True),
            StructField("vertical_rate", DoubleType(), True),
            StructField("sensors", StringType(), True),
            StructField("geo_altitude", DoubleType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
            StructField("position_source", IntegerType(), True),
            StructField("fetch_time", StringType(), True),
            StructField("api_time", IntegerType(), True)
        ])

    def _clean_flight_data(self, df):
        """Очистка и нормализация данных о полетах"""
        return df \
            .withColumn("longitude", when(col("longitude").isNull() | isnan(col("longitude")), lit(0.0)).otherwise(col("longitude"))) \
            .withColumn("latitude", when(col("latitude").isNull() | isnan(col("latitude")), lit(0.0)).otherwise(col("latitude"))) \
            .withColumn("baro_altitude", when(col("baro_altitude").isNull() | isnan(col("baro_altitude")), lit(0.0)).otherwise(col("baro_altitude"))) \
            .withColumn("velocity", when(col("velocity").isNull() | isnan(col("velocity")), lit(0.0)).otherwise(col("velocity"))) \
            .withColumn("true_track", when(col("true_track").isNull() | isnan(col("true_track")), lit(0.0)).otherwise(col("true_track"))) \
            .withColumn("vertical_rate", when(col("vertical_rate").isNull() | isnan(col("vertical_rate")), lit(0.0)).otherwise(col("vertical_rate"))) \
            .withColumn("geo_altitude", when(col("geo_altitude").isNull() | isnan(col("geo_altitude")), lit(0.0)).otherwise(col("geo_altitude"))) \
            .withColumn("on_ground", when(col("on_ground").isNull(), lit(False)).otherwise(col("on_ground"))) \
            .withColumn("spi", when(col("spi").isNull(), lit(False)).otherwise(col("spi"))) \
            .withColumn("callsign", when(col("callsign").isNull(), lit("UNKNOWN")).otherwise(col("callsign"))) \
            .withColumn("origin_country", when(col("origin_country").isNull(), lit("UNKNOWN")).otherwise(col("origin_country")))

    def _add_derived_columns(self, df, batch_id: str):
        """Добавление вычисляемых колонок"""
        return df \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("is_valid_position",
                       (col("longitude") != 0.0) & (col("latitude") != 0.0) &
                       (col("longitude").between(-180, 180)) & (col("latitude").between(-90, 90))) \
            .withColumn("altitude_category",
                       when(col("baro_altitude") < 1000, "low")
                       .when(col("baro_altitude") < 10000, "medium")
                       .when(col("baro_altitude") < 20000, "high")
                       .otherwise("very_high")) \
            .withColumn("speed_category",
                       when(col("velocity") < 100, "slow")
                       .when(col("velocity") < 500, "medium")
                       .otherwise("fast")) \
            .withColumn("year", year(col("fetch_time"))) \
            .withColumn("month", month(col("fetch_time"))) \
            .withColumn("day", dayofmonth(col("fetch_time"))) \
            .withColumn("hour", hour(col("fetch_time")))

    def process_from_s3(self, s3_path: str, batch_id: str = None) -> None:
        """
        Обработка данных из S3 (MinIO)
        Это типичный batch сценарий - обработка больших объемов исторических данных
        """
        if batch_id is None:
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            logger.info(f"Начинаем batch обработку из S3: {s3_path}")

            # Читаем данные из S3 (MinIO)
            df = self.spark.read \
                .option("multiline", "true") \
                .json(s3_path)

            logger.info(f"Загружено {df.count()} записей из S3")

            # Очищаем данные
            cleaned_df = self._clean_flight_data(df)

            # Добавляем вычисляемые колонки
            enriched_df = self._add_derived_columns(cleaned_df, batch_id)

            # Фильтруем валидные позиции
            valid_flights = enriched_df.filter(col("is_valid_position") == True)

            logger.info(f"Валидных рейсов: {valid_flights.count()}")

            # Сохраняем в ClickHouse (batch таблица)
            self._save_to_clickhouse_batch(valid_flights)

            # Сохраняем в S3 в обработанном виде (для дальнейшего анализа)
            output_path = f"s3a://processed-data/batch/{batch_id}"
            valid_flights.write \
                .mode("overwrite") \
                .parquet(output_path)

            logger.info(f"Batch обработка завершена. Результаты сохранены в {output_path}")

            # Создаем агрегации для аналитики
            self._create_aggregations(valid_flights, batch_id)

        except Exception as e:
            logger.error(f"Ошибка batch обработки: {e}")
            raise

    def process_from_postgres(self, start_date: str, end_date: str, batch_id: str = None) -> None:
        """
        Обработка исторических данных из PostgreSQL
        Еще один пример batch обработки
        """
        if batch_id is None:
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            logger.info(f"Начинаем batch обработку из PostgreSQL: {start_date} - {end_date}")

            # Читаем данные из PostgreSQL
            df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}") \
                .option("dbtable", "processed_flights") \
                .option("user", settings.postgres_user) \
                .option("password", settings.postgres_password) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            # Фильтруем по датам
            filtered_df = df.filter(
                (col("fetch_time") >= start_date) &
                (col("fetch_time") <= end_date)
            )

            logger.info(f"Загружено {filtered_df.count()} записей из PostgreSQL")

            # Добавляем batch_id
            enriched_df = filtered_df.withColumn("batch_id", lit(batch_id))

            # Сохраняем в ClickHouse
            self._save_to_clickhouse_batch(enriched_df)

            # Сохраняем в S3
            output_path = f"s3a://processed-data/batch/{batch_id}"
            enriched_df.write \
                .mode("overwrite") \
                .parquet(output_path)

            logger.info(f"Batch обработка завершена")

        except Exception as e:
            logger.error(f"Ошибка batch обработки: {e}")
            raise

    def _save_to_clickhouse_batch(self, df):
        """Сохранение batch данных в ClickHouse"""
        try:
            # ClickHouse JDBC connection
            df.write \
                .format("jdbc") \
                .option("url", "jdbc:clickhouse://clickhouse:8123/opensky") \
                .option("dbtable", "flights_batch") \
                .option("user", "default") \
                .option("password", "clickhouse_password") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()

            logger.info("Данные сохранены в ClickHouse (batch таблица)")

        except Exception as e:
            logger.error(f"Ошибка сохранения в ClickHouse: {e}")
            # Fallback: сохраняем в PostgreSQL
            self._save_to_postgres(df)

    def _save_to_postgres(self, df):
        """Сохранение в PostgreSQL как fallback"""
        try:
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}") \
                .option("dbtable", "processed_flights") \
                .option("user", settings.postgres_user) \
                .option("password", settings.postgres_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            logger.info("Данные сохранены в PostgreSQL")

        except Exception as e:
            logger.error(f"Ошибка сохранения в PostgreSQL: {e}")

    def _create_aggregations(self, df, batch_id: str):
        """Создание агрегаций для аналитики"""
        try:
            # Агрегация по странам и часам
            country_stats = df \
                .groupBy("origin_country", "year", "month", "day", "hour") \
                .agg(
                    count("*").alias("flight_count"),
                    countDistinct("icao24").alias("unique_aircraft"),
                    avg("velocity").alias("avg_velocity"),
                    avg("baro_altitude").alias("avg_altitude"),
                    spark_max("velocity").alias("max_velocity"),
                    spark_min("velocity").alias("min_velocity"),
                    count(when(col("on_ground") == True, 1)).alias("grounded_count"),
                    count(when(col("on_ground") == False, 1)).alias("airborne_count")
                ) \
                .withColumn("batch_id", lit(batch_id))

            # Сохраняем агрегации в S3
            stats_path = f"s3a://processed-data/aggregations/{batch_id}"
            country_stats.write \
                .mode("overwrite") \
                .parquet(stats_path)

            logger.info(f"Агрегации сохранены в {stats_path}")

        except Exception as e:
            logger.error(f"Ошибка создания агрегаций: {e}")

    def stop(self):
        """Остановка Spark Session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark Batch Session закрыт")


def main():
    """Основная функция для batch обработки"""
    import sys

    processor = BatchFlightProcessor()

    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == "s3":
                # Обработка из S3
                s3_path = sys.argv[2] if len(sys.argv) > 2 else "s3a://raw-data/flights/*.json"
                processor.process_from_s3(s3_path)
            elif sys.argv[1] == "postgres":
                # Обработка из PostgreSQL
                start_date = sys.argv[2] if len(sys.argv) > 2 else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                end_date = sys.argv[3] if len(sys.argv) > 3 else datetime.now().strftime("%Y-%m-%d")
                processor.process_from_postgres(start_date, end_date)
            else:
                print("Использование: batch_processor.py [s3|postgres] [args...]")
        else:
            # По умолчанию обрабатываем из S3
            processor.process_from_s3("s3a://raw-data/flights/*.json")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        processor.stop()
        raise
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
