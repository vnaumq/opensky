"""
Spark Structured Streaming job для обработки данных о полетах в реальном времени
Демонстрирует различия между batch и streaming обработкой
"""
import json
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, isnan, isnull,
    lit, current_timestamp, window, count, countDistinct, avg, max as spark_max,
    min as spark_min, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

from config.settings import settings
from config.logging import get_logger

logger = get_logger(__name__)


class StreamingFlightProcessor:
    """Обработчик потоковых данных о полетах с использованием Spark Structured Streaming"""

    def __init__(self):
        self.spark = None
        self._setup_spark()

    def _setup_spark(self) -> None:
        """Настройка Spark Session для streaming"""
        try:
            self.spark = SparkSession.builder \
                .appName("opensky_streaming_processor") \
                .master(settings.spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.streaming.checkpointLocation", "/opt/bitnami/spark/data/checkpoints") \
                .config("spark.sql.streaming.schemaInference", "true") \
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark Streaming Session создан успешно")

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

    def _add_derived_columns(self, df):
        """Добавление вычисляемых колонок"""
        return df \
            .withColumn("processing_time", current_timestamp()) \
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
                       .otherwise("fast"))

    def process_streaming_from_kafka(self, batch_interval: str = "10 seconds"):
        """
        Обработка потоковых данных из Kafka
        Это типичный streaming сценарий - обработка данных в реальном времени
        """
        try:
            logger.info("Начинаем streaming обработку из Kafka")

            # Читаем поток из Kafka
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
                .option("subscribe", settings.kafka_topic_raw_flights) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()

            # Парсим JSON из Kafka
            value_schema = self._get_flight_schema()
            flights_df = kafka_df \
                .select(from_json(col("value").cast("string"), value_schema).alias("data")) \
                .select("data.*")

            # Очищаем данные
            cleaned_df = self._clean_flight_data(flights_df)

            # Добавляем вычисляемые колонки
            enriched_df = self._add_derived_columns(cleaned_df)

            # Фильтруем валидные позиции
            valid_flights = enriched_df.filter(col("is_valid_position") == True)

            # Создаем агрегации по окнам времени (sliding window)
            windowed_stats = valid_flights \
                .withWatermark("processing_time", "1 minute") \
                .groupBy(
                    window(col("processing_time"), "5 minutes", "1 minute"),
                    col("origin_country"),
                    col("altitude_category")
                ) \
                .agg(
                    count("*").alias("flight_count"),
                    countDistinct("icao24").alias("unique_aircraft"),
                    avg("velocity").alias("avg_velocity"),
                    avg("baro_altitude").alias("avg_altitude"),
                    spark_max("velocity").alias("max_velocity"),
                    spark_min("velocity").alias("min_velocity"),
                    count(when(col("on_ground") == True, 1)).alias("grounded_count"),
                    count(when(col("on_ground") == False, 1)).alias("airborne_count")
                )

            # Сохраняем в ClickHouse (streaming таблица)
            clickhouse_query = valid_flights \
                .writeStream \
                .outputMode("append") \
                .foreachBatch(self._save_to_clickhouse_streaming) \
                .trigger(processingTime=batch_interval) \
                .start()

            # Сохраняем агрегации в S3 (для дальнейшего batch анализа)
            s3_query = windowed_stats \
                .writeStream \
                .outputMode("update") \
                .format("parquet") \
                .option("path", "s3a://streaming-aggregations/") \
                .option("checkpointLocation", "/opt/bitnami/spark/data/checkpoints/s3") \
                .partitionBy("origin_country", "altitude_category") \
                .trigger(processingTime=batch_interval) \
                .start()

            # Также сохраняем в PostgreSQL для совместимости
            postgres_query = valid_flights \
                .writeStream \
                .outputMode("append") \
                .foreachBatch(self._save_to_postgres) \
                .trigger(processingTime=batch_interval) \
                .start()

            logger.info("Streaming queries запущены")

            # Ждем завершения
            clickhouse_query.awaitTermination()
            s3_query.awaitTermination()
            postgres_query.awaitTermination()

        except Exception as e:
            logger.error(f"Ошибка streaming обработки: {e}")
            raise

    def _save_to_clickhouse_streaming(self, batch_df, batch_id):
        """Сохранение streaming данных в ClickHouse"""
        try:
            # ClickHouse JDBC connection
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:clickhouse://clickhouse:8123/opensky") \
                .option("dbtable", "flights_streaming") \
                .option("user", "default") \
                .option("password", "clickhouse_password") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()

            logger.info(f"Batch {batch_id}: Сохранено {batch_df.count()} записей в ClickHouse")

        except Exception as e:
            logger.error(f"Ошибка сохранения в ClickHouse: {e}")

    def _save_to_postgres(self, batch_df, batch_id):
        """Сохранение в PostgreSQL"""
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}") \
                .option("dbtable", "processed_flights") \
                .option("user", settings.postgres_user) \
                .option("password", settings.postgres_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            logger.info(f"Batch {batch_id}: Сохранено в PostgreSQL")

        except Exception as e:
            logger.error(f"Ошибка сохранения в PostgreSQL: {e}")

    def stop(self):
        """Остановка Spark Session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark Streaming Session закрыт")


def main():
    """Основная функция для streaming обработки"""
    processor = StreamingFlightProcessor()

    try:
        # Настраиваем обработку потоковых данных
        processor.process_streaming_from_kafka(batch_interval="10 seconds")

    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
        processor.stop()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        processor.stop()
        raise


if __name__ == "__main__":
    main()
