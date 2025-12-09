from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import *
from kafka import KafkaConsumer
import json
import config

spark = SparkSession.builder \
    .appName("OpenSky-Kafka-to-PostgreSQL") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"  # Kafka
            "org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
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
        flight = message.value  # ← Это уже готовый словарь! Не список!

        # Защита от мусора
        if not isinstance(flight, dict):
            print("Пропускаем не-словарь:", flight)
            continue

        # Чистим callsign от пробелов
        if flight.get("callsign"):
            flight["callsign"] = flight["callsign"].strip()
        else:
            flight["callsign"] = None

        # Приводим числовые поля к float (на всякий случай, если где-то int)
        numeric_fields = ["longitude", "latitude", "baro_altitude", "velocity",
                          "true_track", "vertical_rate", "geo_altitude"]
        for field in numeric_fields:
            if flight.get(field) is not None:
                flight[field] = float(flight[field])

        # Красивый вывод
        callsign = flight.get("callsign") or "N/A"
        icao24 = flight.get("icao24", "unknown")
        lat = flight.get("latitude")
        lon = flight.get("longitude")

        print(f"RECEIVED: {callsign:<8} ({icao24})")

        # Добавляем в батч
        batch.append(flight)

        # Сохраняем батч
        if len(batch) >= BATCH_SIZE:
            df = spark.createDataFrame(batch, schema=schema) \
                      .withColumn("ingestion_time", current_timestamp())

            df.write.mode("append").jdbc(url=jdbc_url, table="flights_raw", properties=properties)
            print(f"SAVED {len(batch)} records to PostgreSQL")
            batch.clear()

except KeyboardInterrupt:
    print("\nОстанавливаем консюмер...")
finally:
    if batch:
        df = spark.createDataFrame(batch, schema=schema) \
                  .withColumn("ingestion_time", current_timestamp())
        df.write.mode("append").jdbc(url=jdbc_url, table="flights_raw", properties=properties)
        print(f"FINAL SAVE: {len(batch)} records")

    consumer.close()
    spark.stop()
    print("Готово. Все данные сохранены.")