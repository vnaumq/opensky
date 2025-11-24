# OpenSky Flight Data Pipeline

Полнофункциональный пайплайн для сбора, обработки и анализа данных о полетах с использованием OpenSky API.

## 🚀 Возможности

- **Сбор данных**: Автоматический сбор данных о полетах в реальном времени через OpenSky API
- **Потоковая обработка**: Обработка данных с использованием Apache Kafka и Spark Streaming
- **Batch обработка**: Обработка исторических данных с использованием Apache Spark Batch
- **CDC (Change Data Capture)**: Отслеживание изменений в PostgreSQL через Debezium
- **Аналитическое хранилище**: ClickHouse для быстрой аналитики больших объемов данных
- **Object Storage**: MinIO (S3-compatible) для хранения сырых и обработанных данных
- **Трансформация данных**: DBT модели для создания аналитических таблиц
- **Оркестрация**: Apache Airflow для автоматизации ETL процессов
- **Мониторинг**: Prometheus и Grafana для мониторинга системы
- **Обучение**: Jupyter Notebooks для изучения технологий и экспериментов
- **Визуализация**: Дашборды для анализа активности полетов
- **Контейнеризация**: Полная контейнеризация с Docker

## 🏗️ Архитектура

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   OpenSky API   │───▶│  Data Collector │───▶│     Kafka      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                        ┌───────────────────────────────┘
                        │                               │
                        ▼                               ▼
              ┌─────────────────┐            ┌─────────────────┐
              │   Debezium CDC  │            │  Spark Streaming│
              │  (PostgreSQL→Kafka)          │  (Real-time)    │
              └─────────────────┘            └─────────────────┘
                        │                               │
                        ▼                               ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Grafana     │◀───│   Prometheus    │    │  Spark Batch    │
└─────────────────┘    └─────────────────┘    │  (Historical)   │
                                │               └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │◀───│      DBT        │    │   ClickHouse    │
└─────────────────┘    └─────────────────┘    │  (Analytics DB) │
                                                └─────────────────┘
                        ┌─────────────────┐            │
                        │   MinIO (S3)    │◀───────────┘
                        │  (Object Store) │
                        └─────────────────┘
```

## 🛠️ Технологический стек

- **Сбор данных**: Python, aiohttp, OpenSky API
- **Потоковая обработка**: Apache Kafka, Apache Spark Streaming
- **Batch обработка**: Apache Spark Batch
- **CDC (Change Data Capture)**: Debezium
- **Хранилище**: PostgreSQL, ClickHouse
- **Object Storage**: MinIO (S3-compatible)
- **Трансформация**: DBT
- **Оркестрация**: Apache Airflow
- **Мониторинг**: Prometheus, Grafana
- **Разработка и обучение**: Jupyter Notebooks
- **Контейнеризация**: Docker, Docker Compose

## 📋 Требования

- Docker и Docker Compose
- Минимум 4GB RAM
- Минимум 10GB свободного места на диске
- Доступ к интернету для OpenSky API

## 🚀 Быстрый старт

### 1. Клонирование и настройка

```bash
git clone <repository-url>
cd opensky
chmod +x scripts/*.sh
```

### 2. Настройка окружения

```bash
# Копирование файла конфигурации
cp .env.example .env

# Редактирование настроек
nano .env
```

Настройте следующие переменные в `.env`:
```env
OPENSKY_CLIENT_ID=your_client_id
OPENSKY_CLIENT_SECRET=your_client_secret
POSTGRES_PASSWORD=your_secure_password
```

### 3. Запуск проекта

```bash
# Первоначальная настройка
./scripts/setup.sh

# Запуск сервисов
./scripts/start.sh
```

### 4. Доступ к сервисам

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Jupyter Notebooks**: http://localhost:8888
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **ClickHouse**: http://localhost:8123 (default/clickhouse_password)
- **Debezium Connect**: http://localhost:8083
- **Spark Master UI**: http://localhost:8081
- **PostgreSQL**: localhost:5432
- **Kafka**: localhost:9092

## 📊 Использование

### Запуск сбора данных

1. Откройте Airflow UI (http://localhost:8080)
2. Найдите DAG `opensky_data_pipeline`
3. Включите и запустите DAG
4. Мониторьте выполнение задач

### Просмотр аналитики

1. Откройте Grafana (http://localhost:3000)
2. Перейдите в раздел "Dashboards"
3. Выберите "OpenSky Flight Data Dashboard"
4. Анализируйте метрики в реальном времени

### Мониторинг системы

1. Откройте Prometheus (http://localhost:9090)
2. Проверьте метрики системы
3. Настройте алерты при необходимости

### Работа с Jupyter Notebooks

1. Откройте Jupyter (http://localhost:8888)
2. Изучите notebooks в директории `jupyter/`:
   - `01_batch_vs_streaming.ipynb` - Различия между batch и streaming
   - `02_kafka_basics.ipynb` - Основы работы с Kafka
   - `03_clickhouse_basics.ipynb` - Работа с ClickHouse
   - `04_debezium_cdc.ipynb` - CDC с Debezium
   - `05_s3_minio.ipynb` - Работа с S3/MinIO

### Настройка Debezium CDC

1. Запустите скрипт настройки:
   ```bash
   ./scripts/setup_debezium.sh
   ```
2. Проверьте статус connector:
   ```bash
   curl http://localhost:8083/connectors/opensky-postgres-connector/status
   ```
3. Изменения в PostgreSQL будут автоматически отправляться в Kafka

### Запуск Batch обработки

```bash
# Обработка данных из S3
spark-submit --master spark://spark-master:7077 \
  spark_jobs/batch_processor.py s3 s3a://raw-data/flights/*.json

# Обработка данных из PostgreSQL
spark-submit --master spark://spark-master:7077 \
  spark_jobs/batch_processor.py postgres 2024-01-01 2024-01-31
```

### Запуск Streaming обработки

```bash
spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_jobs/streaming_processor.py
```

## 📁 Структура проекта

```
opensky/
├── src/                          # Исходный код
│   ├── data_collector.py         # Сборщик данных
│   ├── kafka_producer.py         # Kafka producer/consumer
│   └── monitoring/               # Модули мониторинга
├── spark_jobs/                   # Spark задачи
│   ├── batch_processor.py        # Batch обработка
│   ├── streaming_processor.py   # Streaming обработка
│   └── flight_processor.py       # Обработчик данных (legacy)
├── dbt/                          # DBT модели
│   ├── models/                   # SQL модели
│   └── dbt_project.yml           # Конфигурация DBT
├── airflow_dags/                # Airflow DAG'и
│   ├── opensky_pipeline.py       # Основной пайплайн
│   └── opensky_monitoring.py     # Мониторинг
├── jupyter/                      # Jupyter notebooks
│   ├── 01_batch_vs_streaming.ipynb
│   ├── 02_kafka_basics.ipynb
│   ├── 03_clickhouse_basics.ipynb
│   ├── 04_debezium_cdc.ipynb
│   └── 05_s3_minio.ipynb
├── monitoring/                   # Конфигурация мониторинга
│   ├── prometheus.yml            # Prometheus конфиг
│   └── grafana/                  # Grafana дашборды
├── config/                       # Конфигурационные файлы
│   ├── clickhouse/               # ClickHouse конфиг
│   └── debezium/                 # Debezium конфиг
├── init_clickhouse/              # Инициализация ClickHouse
│   └── init.sql
├── scripts/                      # Скрипты управления
│   ├── setup.sh                  # Настройка
│   ├── start.sh                  # Запуск
│   ├── stop.sh                   # Остановка
│   └── setup_debezium.sh         # Настройка Debezium
├── docker-compose.yaml           # Docker Compose
└── requirements.txt              # Python зависимости
```

## 🔧 Конфигурация

### Настройка регионов сбора данных

Отредактируйте `src/data_collector.py`:

```python
regions = {
    "europe": {
        "lamin": 35.0, "lomin": -10.0,
        "lamax": 70.0, "lomax": 40.0
    },
    "north_america": {
        "lamin": 25.0, "lomin": -125.0,
        "lamax": 50.0, "lomax": -65.0
    }
}
```

### Настройка мониторинга

Отредактируйте `monitoring/prometheus.yml` для настройки метрик.

### Настройка DBT

Отредактируйте `dbt/models/` для изменения логики трансформации данных.

## 📈 Мониторинг и алерты

### Метрики системы

- CPU и память
- Использование диска
- Сетевая активность
- Количество процессов

### Метрики приложения

- Количество обработанных рейсов
- Качество данных
- Задержка обработки
- Статус сервисов

### Алерты

- Высокое использование ресурсов
- Ошибки в обработке данных
- Недоступность внешних сервисов
- Проблемы с качеством данных

## 🛠️ Разработка

### Добавление новых метрик

1. Добавьте метрику в `src/monitoring/performance_monitor.py`
2. Обновите Grafana дашборд
3. Перезапустите сервисы

### Добавление новых DBT моделей

1. Создайте SQL файл в `dbt/models/`
2. Добавьте тесты в `dbt/tests/`
3. Запустите `dbt run` и `dbt test`

### Добавление новых Airflow задач

1. Отредактируйте `airflow_dags/opensky_pipeline.py`
2. Добавьте новую задачу
3. Настройте зависимости

## 🐛 Устранение неполадок

### Проблемы с подключением к OpenSky API

1. Проверьте credentials в `.env`
2. Убедитесь в наличии интернет-соединения
3. Проверьте лимиты API

### Проблемы с Kafka

1. Проверьте статус контейнеров: `docker-compose ps`
2. Проверьте логи: `docker-compose logs kafka`
3. Перезапустите сервисы: `docker-compose restart kafka`

### Проблемы с базой данных

1. Проверьте подключение к PostgreSQL
2. Проверьте права доступа
3. Проверьте логи: `docker-compose logs postgres`

### Проблемы с ClickHouse

1. Проверьте логи: `docker-compose logs clickhouse`
2. Проверьте подключение: `curl http://localhost:8123`
3. Убедитесь, что таблицы созданы: `docker-compose exec clickhouse clickhouse-client --query "SHOW TABLES"`

### Проблемы с Debezium

1. Проверьте логи: `docker-compose logs debezium-connect`
2. Проверьте статус connector: `curl http://localhost:8083/connectors`
3. Убедитесь, что PostgreSQL настроен для логического реплицирования (wal_level=logical)

### Проблемы с MinIO

1. Проверьте логи: `docker-compose logs minio`
2. Откройте консоль MinIO: http://localhost:9001
3. Убедитесь, что buckets созданы

### Проблемы с Spark

1. Проверьте статус Spark Master: http://localhost:8081
2. Проверьте логи: `docker-compose logs spark-master spark-worker`
3. Убедитесь, что Spark может подключиться к Kafka и другим сервисам

## 📝 Лицензия

MIT License

## 🤝 Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для новой функции
3. Внесите изменения
4. Создайте Pull Request

## 📞 Поддержка

Для вопросов и поддержки создайте Issue в репозитории.
