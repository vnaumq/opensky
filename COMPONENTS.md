# Описание компонентов проекта

## Batch vs Streaming обработка

### Batch обработка (`spark_jobs/batch_processor.py`)

**Характеристики:**
- Обработка больших объемов исторических данных
- Выполняется по расписанию или по запросу
- Обрабатывает все данные сразу
- Более эффективна для больших объемов
- Результаты доступны после завершения

**Использование:**
```bash
# Обработка из S3
spark-submit --master spark://spark-master:7077 \
  spark_jobs/batch_processor.py s3 s3a://raw-data/flights/*.json

# Обработка из PostgreSQL
spark-submit --master spark://spark-master:7077 \
  spark_jobs/batch_processor.py postgres 2024-01-01 2024-01-31
```

**Результаты:**
- Сохраняются в ClickHouse (таблица `flights_batch`)
- Сохраняются в S3 (parquet формат)
- Создаются агрегации для аналитики

### Streaming обработка (`spark_jobs/streaming_processor.py`)

**Характеристики:**
- Обработка данных в реальном времени
- Непрерывная обработка
- Обрабатывает данные небольшими батчами (10 секунд)
- Результаты доступны сразу
- Использует Spark Structured Streaming

**Использование:**
```bash
spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_jobs/streaming_processor.py
```

**Результаты:**
- Сохраняются в ClickHouse (таблица `flights_streaming`)
- Сохраняются в S3 (агрегации)
- Сохраняются в PostgreSQL (для совместимости)

## ClickHouse

Колоночная БД для аналитики больших объемов данных.

**Таблицы:**
- `flights_streaming` - данные из streaming обработки
- `flights_batch` - данные из batch обработки
- `country_statistics_mv` - материализованное представление для агрегаций

**Подключение:**
```python
from clickhouse_driver import Client

client = Client(
    host='clickhouse',
    port=9000,
    user='default',
    password='clickhouse_password',
    database='opensky'
)
```

**Запросы:**
```sql
-- Количество рейсов по странам за последний час
SELECT
    origin_country,
    count() as flight_count,
    avg(velocity) as avg_velocity
FROM flights_streaming
WHERE fetch_time >= now() - INTERVAL 1 HOUR
GROUP BY origin_country
ORDER BY flight_count DESC
```

## Debezium CDC

Отслеживание изменений в PostgreSQL и отправка их в Kafka.

**Настройка:**
```bash
./scripts/setup_debezium.sh
```

**Топики в Kafka:**
- `opensky.public.processed_flights` - изменения в таблице processed_flights
- `opensky.public.flight_statistics` - изменения в таблице flight_statistics

**Формат событий:**
```json
{
  "before": {...},  // Старое значение (для UPDATE/DELETE)
  "after": {...},   // Новое значение (для INSERT/UPDATE)
  "op": "c/u/d",    // Операция: c=create, u=update, d=delete
  "source": {...}   // Метаданные
}
```

## MinIO (S3)

S3-совместимое хранилище для объектов.

**Buckets:**
- `raw-data` - сырые данные
- `processed-data` - обработанные данные
- `streaming-aggregations` - агрегации из streaming

**Подключение:**
```python
import boto3

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123'
)
```

**Использование в Spark:**
```python
spark.read.parquet("s3a://raw-data/flights/*.parquet")
```

## Kafka

Распределенная платформа для потоковой обработки данных.

**Топики:**
- `raw_flights` - сырые данные о полетах
- `processed_flights` - обработанные данные
- `opensky.public.processed_flights` - CDC события из PostgreSQL

**Producer:**
```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
producer.send('raw_flights', value=data)
```

**Consumer:**
```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'raw_flights',
    bootstrap_servers=['kafka:29092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
```

## Jupyter Notebooks

Notebooks для обучения и экспериментов находятся в директории `jupyter/`:

1. **01_batch_vs_streaming.ipynb** - Различия между batch и streaming обработкой
2. **02_kafka_basics.ipynb** - Основы работы с Kafka
3. **03_clickhouse_basics.ipynb** - Работа с ClickHouse
4. **04_debezium_cdc.ipynb** - CDC с Debezium
5. **05_s3_minio.ipynb** - Работа с S3/MinIO

**Доступ:** http://localhost:8888

## Apache Spark

Распределенная система обработки данных.

**Режимы:**
- **Batch** - обработка исторических данных
- **Streaming** - обработка данных в реальном времени

**UI:** http://localhost:8081

**Запуск jobs:**
```bash
# Batch
spark-submit --master spark://spark-master:7077 spark_jobs/batch_processor.py

# Streaming
spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_jobs/streaming_processor.py
```

## Сравнение Batch и Streaming

| Характеристика | Batch | Streaming |
|----------------|-------|-----------|
| Задержка | Высокая (часы/дни) | Низкая (секунды/минуты) |
| Объем данных | Большой | Небольшие батчи |
| Ресурсы | Высокие, но периодические | Постоянные |
| Сложность | Проще | Сложнее |
| Использование | Исторический анализ | Мониторинг в реальном времени |
| Хранилище | ClickHouse (batch таблица), S3 | ClickHouse (streaming таблица) |

## Рекомендации по использованию

1. **Streaming** - для мониторинга в реальном времени, алертов, дашбордов
2. **Batch** - для исторического анализа, отчетов, ML моделей
3. **ClickHouse** - для быстрой аналитики больших объемов данных
4. **PostgreSQL** - для транзакционных данных и DBT трансформаций
5. **S3** - для хранения сырых данных и результатов обработки
6. **Debezium** - для синхронизации данных между системами в реальном времени
