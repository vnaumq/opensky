"""
Spark Streaming job для обработки данных о полетах
"""
import json
from datetime import datetime, timezone
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, isnan, isnull,
    lit, current_timestamp, window, count, avg, max as spark_max,
    min as spark_min, collect_list, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType, TimestampType
)
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from config.settings import settings
from config.logging import get_logger

logger = get_logger(__name__)


class FlightDataProcessor:
    """Обработчик данных о полетах с использованием Spark Streaming"""
    
    def __init__(self):
        self.spark = None
        self.ssc = None
        self._setup_spark()
    
    def _setup_spark(self) -> None:
        """Настройка Spark Session"""
        try:
            self.spark = SparkSession.builder \
                .appName(settings.spark_app_name) \
                .master(settings.spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark Session создан успешно")
            
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
    
    def _aggregate_flight_stats(self, df):
        """Агрегация статистики по полетам"""
        return df \
            .groupBy("origin_country", "altitude_category") \
            .agg(
                count("*").alias("flight_count"),
                avg("velocity").alias("avg_velocity"),
                avg("baro_altitude").alias("avg_altitude"),
                spark_max("velocity").alias("max_velocity"),
                spark_min("velocity").alias("min_velocity"),
                count(when(col("on_ground") == True, 1)).alias("grounded_count"),
                count(when(col("on_ground") == False, 1)).alias("airborne_count")
            )
    
    def process_streaming_data(self, batch_interval: int = 10):
        """Обработка потоковых данных"""
        try:
            # Создаем StreamingContext
            self.ssc = StreamingContext(self.spark.sparkContext, batch_interval)
            
            # Настройка Kafka
            kafka_params = {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "auto.offset.reset": "latest"
            }
            
            # Создаем DStream из Kafka
            kafka_stream = KafkaUtils.createDirectStream(
                self.ssc,
                [settings.kafka_topic_raw_flights],
                kafka_params
            )
            
            # Извлекаем сообщения
            messages = kafka_stream.map(lambda x: x[1])
            
            def process_batch(rdd):
                """Обработка батча данных"""
                if rdd.isEmpty():
                    logger.info("Пустой батч данных")
                    return
                
                try:
                    # Преобразуем RDD в DataFrame
                    json_rdd = rdd.map(lambda x: json.loads(x))
                    df = self.spark.createDataFrame(json_rdd, self._get_flight_schema())
                    
                    # Очищаем данные
                    cleaned_df = self._clean_flight_data(df)
                    
                    # Добавляем вычисляемые колонки
                    enriched_df = self._add_derived_columns(cleaned_df)
                    
                    # Фильтруем валидные позиции
                    valid_flights = enriched_df.filter(col("is_valid_position") == True)
                    
                    # Агрегируем статистику
                    stats_df = self._aggregate_flight_stats(valid_flights)
                    
                    # Сохраняем результаты
                    self._save_processed_data(valid_flights, stats_df)
                    
                    logger.info(f"Обработано {valid_flights.count()} рейсов")
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки батча: {e}")
            
            # Применяем обработку к каждому батчу
            messages.foreachRDD(process_batch)
            
            logger.info("Streaming job запущен")
            
        except Exception as e:
            logger.error(f"Ошибка настройки streaming: {e}")
            raise
    
    def _save_processed_data(self, flights_df, stats_df):
        """Сохранение обработанных данных"""
        try:
            # Сохраняем в PostgreSQL
            flights_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}") \
                .option("dbtable", "processed_flights") \
                .option("user", settings.postgres_user) \
                .option("password", settings.postgres_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            # Сохраняем статистику
            stats_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}") \
                .option("dbtable", "flight_statistics") \
                .option("user", settings.postgres_user) \
                .option("password", settings.postgres_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            logger.info("Данные сохранены в PostgreSQL")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
    
    def start_streaming(self):
        """Запуск streaming job"""
        try:
            self.ssc.start()
            self.ssc.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки")
            self.stop_streaming()
        except Exception as e:
            logger.error(f"Ошибка в streaming job: {e}")
            self.stop_streaming()
    
    def stop_streaming(self):
        """Остановка streaming job"""
        if self.ssc:
            self.ssc.stop(stopSparkContext=False)
            logger.info("Streaming job остановлен")
        
        if self.spark:
            self.spark.stop()
            logger.info("Spark Session закрыт")


def main():
    """Основная функция"""
    processor = FlightDataProcessor()
    
    try:
        # Настраиваем обработку потоковых данных
        processor.process_streaming_data(batch_interval=10)
        
        # Запускаем streaming
        processor.start_streaming()
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        processor.stop_streaming()


if __name__ == "__main__":
    main()
