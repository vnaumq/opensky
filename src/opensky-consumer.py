from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import *
from kafka import KafkaConsumer
import json
import config

spark = SparkSession.builder \
    .appName("OpenSky-to-PostgreSQL") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-3.5.0:3.5.0,"  # Kafka integration
            "org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("icao24", StringType(), False),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", LongType(), True),           # time_position
    StructField("last_contact", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("sensors", ArrayType(IntegerType()), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", IntegerType(), True),
    # StructField("category", IntegerType(), True),  # если есть 18-й элемент
])

jdbc_url, properties = config.get_jdbc_config()

print('CONNECTING TO KAFKA...')

consumer = KafkaConsumer(
    "opensky",
    bootstrap_servers=["kafka:29092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id="spark-group"
)

print('CONNECTED TO KAFKA')
print("\n" + "="*50)
print('👁️  WAITING....')
print("="*50 + "\n")

batch = []
BATCH_SIZE = 100

try:
    for message in consumer:
        raw_flight = message.value  # это список из 17+ элементов

        # Защита от кривых сообщений
        if not isinstance(raw_flight, list) or len(raw_flight) < 17:
            print("Bad message, skipping:", raw_flight)
            continue

        # Формируем чистый словарь (обрабатываем None)
        flight = {
            "icao24": str(raw_flight[0]).strip().lower() if raw_flight[0] else None,
            "callsign": str(raw_flight[1]).strip() if raw_flight[1] else None,
            "origin_country": raw_flight[2],
            "time_position": raw_flight[3],
            "last_contact": raw_flight[4],
            "longitude": raw_flight[5],
            "latitude": raw_flight[6],
            "baro_altitude": raw_flight[7],
            "on_ground": raw_flight[8],
            "velocity": raw_flight[9],
            "true_track": raw_flight[10],
            "vertical_rate": raw_flight[11],
            "sensors": raw_flight[12],
            "geo_altitude": raw_flight[13],
            "squawk": raw_flight[14],
            "spi": raw_flight[15],
            "position_source": raw_flight[16],
            # "ingestion_time": None  # добавим через Spark
        }

        callsign = flight["callsign"] or "NO_CALLSIGN"
        icao = flight["icao24"] or "unknown"

        print(f"RECEIVED: {callsign} ({icao}) @ {flight['latitude']:.4f}, {flight['longitude']:.4f}")

        batch.append(flight)

        # Когда набрали батч — пишем в БД
        if len(batch) >= BATCH_SIZE:
            # Создаём DataFrame
            df = spark.createDataFrame(batch, schema=schema)
            df = df.withColumn("ingestion_time", current_timestamp())

            # Записываем
            df.write \
                .mode("append") \
                .jdbc(url=jdbc_url, table="flights_raw", properties=properties)

            print(f"SAVED {len(batch)} records to PostgreSQL")
            batch.clear()

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    # Сохраняем остатки
    if batch:
        df = spark.createDataFrame(batch, schema=schema) \
              .withColumn("ingestion_time", current_timestamp())
        df.write.mode("append").jdbc(url=jdbc_url, table="flights_raw", properties=properties)
        print(f"FINAL SAVE: {len(batch)} records")

    consumer.close()
    spark.stop()
    print("Consumer stopped. All data saved.")