"""
Airflow DAG для пайплайна обработки данных OpenSky
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

# Параметры по умолчанию
default_args = {
    'owner': 'opensky_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Создание DAG
dag = DAG(
    'opensky_data_pipeline',
    default_args=default_args,
    description='Пайплайн обработки данных о полетах OpenSky',
    schedule_interval='*/10 * * * *',  # Каждые 10 минут
    max_active_runs=1,
    tags=['opensky', 'aviation', 'streaming']
)

# Задачи DAG

# 1. Сбор данных
collect_data_task = PythonOperator(
    task_id='collect_flight_data',
    python_callable=lambda: __import__('src.data_collector').main(),
    dag=dag,
    pool='data_collection_pool'
)

# 2. Проверка качества данных
data_quality_check = PostgresOperator(
    task_id='data_quality_check',
    postgres_conn_id='postgres_opensky',
    sql="""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN longitude IS NOT NULL AND latitude IS NOT NULL THEN 1 END) as valid_positions,
        COUNT(CASE WHEN callsign IS NOT NULL THEN 1 END) as records_with_callsign,
        COUNT(CASE WHEN origin_country IS NOT NULL THEN 1 END) as records_with_country
    FROM processed_flights 
    WHERE fetch_time >= NOW() - INTERVAL '1 hour'
    """,
    dag=dag
)

# 3. Обработка данных Spark
spark_processing = DockerOperator(
    task_id='spark_flight_processing',
    image='opensky/spark-processor:latest',
    command='python /app/spark_jobs/flight_processor.py',
    network_mode='bridge',
    environment={
        'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
        'POSTGRES_HOST': 'postgres',
        'POSTGRES_DB': 'opensky',
        'POSTGRES_USER': 'opensky_user',
        'POSTGRES_PASSWORD': 'opensky_password'
    },
    dag=dag,
    auto_remove=True
)

# 4. Запуск DBT трансформаций
dbt_run = BashOperator(
    task_id='dbt_run_models',
    bash_command="""
    cd /opt/airflow/dbt && \
    dbt run --profiles-dir . --target dev
    """,
    dag=dag
)

# 5. Тестирование DBT моделей
dbt_test = BashOperator(
    task_id='dbt_test_models',
    bash_command="""
    cd /opt/airflow/dbt && \
    dbt test --profiles-dir . --target dev
    """,
    dag=dag
)

# 6. Обновление метрик
update_metrics = PostgresOperator(
    task_id='update_flight_metrics',
    postgres_conn_id='postgres_opensky',
    sql="""
    INSERT INTO flight_metrics (
        metric_name, 
        metric_value, 
        metric_timestamp,
        additional_data
    )
    SELECT 
        'total_flights_last_hour',
        COUNT(*),
        NOW(),
        json_build_object('geographic_zone', geographic_zone)
    FROM agg_flight_statistics 
    WHERE day_bucket >= NOW() - INTERVAL '1 hour'
    GROUP BY geographic_zone;
    """,
    dag=dag
)

# 7. Очистка старых данных
cleanup_old_data = PostgresOperator(
    task_id='cleanup_old_data',
    postgres_conn_id='postgres_opensky',
    sql="""
    DELETE FROM processed_flights 
    WHERE fetch_time < NOW() - INTERVAL '7 days';
    
    DELETE FROM flight_statistics 
    WHERE created_at < NOW() - INTERVAL '30 days';
    """,
    dag=dag
)

# 8. Уведомление о завершении
notify_completion = PythonOperator(
    task_id='notify_pipeline_completion',
    python_callable=lambda: print("Пайплайн OpenSky успешно завершен"),
    dag=dag
)

# Определение зависимостей
collect_data_task >> data_quality_check
data_quality_check >> spark_processing
spark_processing >> dbt_run
dbt_run >> dbt_test
dbt_test >> update_metrics
update_metrics >> cleanup_old_data
cleanup_old_data >> notify_completion
