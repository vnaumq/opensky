# OpenSky Flight Data Pipeline

Полнофункциональный пайплайн для сбора, обработки и анализа данных о полетах с использованием OpenSky API.

## 🚀 Возможности

- **Сбор данных**: Автоматический сбор данных о полетах в реальном времени через OpenSky API
- **Потоковая обработка**: Обработка данных с использованием Apache Kafka и Spark Streaming
- **Трансформация данных**: DBT модели для создания аналитических таблиц
- **Оркестрация**: Apache Airflow для автоматизации ETL процессов
- **Мониторинг**: Prometheus и Grafana для мониторинга системы
- **Визуализация**: Дашборды для анализа активности полетов
- **Контейнеризация**: Полная контейнеризация с Docker

## 🏗️ Архитектура

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   OpenSky API   │───▶│  Data Collector │───▶│     Kafka      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Grafana     │◀───│   Prometheus    │◀───│  Spark Streaming│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │◀───│      DBT        │◀───│    Airflow     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Технологический стек

- **Сбор данных**: Python, aiohttp, OpenSky API
- **Потоковая обработка**: Apache Kafka, Apache Spark Streaming
- **Хранилище**: PostgreSQL
- **Трансформация**: DBT
- **Оркестрация**: Apache Airflow
- **Мониторинг**: Prometheus, Grafana
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
- **PostgreSQL**: localhost:5432

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

## 📁 Структура проекта

```
opensky/
├── src/                          # Исходный код
│   ├── data_collector.py         # Сборщик данных
│   ├── kafka_producer.py         # Kafka producer/consumer
│   └── monitoring/               # Модули мониторинга
├── spark_jobs/                   # Spark задачи
│   └── flight_processor.py       # Обработчик данных
├── dbt/                          # DBT модели
│   ├── models/                   # SQL модели
│   └── dbt_project.yml           # Конфигурация DBT
├── airflow_dags/                # Airflow DAG'и
│   ├── opensky_pipeline.py       # Основной пайплайн
│   └── opensky_monitoring.py     # Мониторинг
├── monitoring/                   # Конфигурация мониторинга
│   ├── prometheus.yml            # Prometheus конфиг
│   └── grafana/                  # Grafana дашборды
├── scripts/                      # Скрипты управления
│   ├── setup.sh                  # Настройка
│   ├── start.sh                  # Запуск
│   └── stop.sh                   # Остановка
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

## 📝 Лицензия

MIT License

## 🤝 Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для новой функции
3. Внесите изменения
4. Создайте Pull Request

## 📞 Поддержка

Для вопросов и поддержки создайте Issue в репозитории.
