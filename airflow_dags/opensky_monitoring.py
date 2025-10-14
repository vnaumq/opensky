"""
DAG для мониторинга системы OpenSky
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.sql import SqlSensor

default_args = {
    'owner': 'opensky_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'opensky_monitoring',
    default_args=default_args,
    description='Мониторинг системы OpenSky',
    schedule_interval='*/5 * * * *',  # Каждые 5 минут
    max_active_runs=1,
    tags=['monitoring', 'opensky']
)

# Проверка доступности данных
check_data_availability = SqlSensor(
    task_id='check_data_availability',
    conn_id='postgres_opensky',
    sql="SELECT COUNT(*) FROM processed_flights WHERE fetch_time >= NOW() - INTERVAL '15 minutes'",
    timeout=60,
    poke_interval=30,
    dag=dag
)

# Проверка качества данных
data_quality_monitor = PostgresOperator(
    task_id='data_quality_monitor',
    postgres_conn_id='postgres_opensky',
    sql="""
    WITH quality_metrics AS (
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN longitude IS NULL OR latitude IS NULL THEN 1 END) as invalid_positions,
            COUNT(CASE WHEN callsign IS NULL OR callsign = '' THEN 1 END) as missing_callsigns,
            COUNT(CASE WHEN origin_country IS NULL THEN 1 END) as missing_countries
        FROM processed_flights 
        WHERE fetch_time >= NOW() - INTERVAL '1 hour'
    )
    INSERT INTO monitoring_alerts (
        alert_type,
        alert_message,
        severity,
        created_at
    )
    SELECT 
        CASE 
            WHEN invalid_positions::float / total_records > 0.1 THEN 'data_quality'
            WHEN missing_callsigns::float / total_records > 0.2 THEN 'data_completeness'
            ELSE 'ok'
        END,
        CASE 
            WHEN invalid_positions::float / total_records > 0.1 THEN 'High percentage of invalid positions'
            WHEN missing_callsigns::float / total_records > 0.2 THEN 'High percentage of missing callsigns'
            ELSE 'Data quality is good'
        END,
        CASE 
            WHEN invalid_positions::float / total_records > 0.1 THEN 'high'
            WHEN missing_callsigns::float / total_records > 0.2 THEN 'medium'
            ELSE 'low'
        END,
        NOW()
    FROM quality_metrics;
    """,
    dag=dag
)

# Проверка производительности системы
performance_monitor = PythonOperator(
    task_id='performance_monitor',
    python_callable=lambda: __import__('src.monitoring.performance_monitor').check_system_performance(),
    dag=dag
)

# Проверка доступности внешних сервисов
external_services_check = PythonOperator(
    task_id='external_services_check',
    python_callable=lambda: __import__('src.monitoring.service_monitor').check_external_services(),
    dag=dag
)

# Отправка уведомлений при критических ошибках
send_critical_alerts = EmailOperator(
    task_id='send_critical_alerts',
    to=['admin@opensky.com'],
    subject='OpenSky System Alert',
    html_content="""
    <h2>Критическое предупреждение системы OpenSky</h2>
    <p>Обнаружены проблемы в системе мониторинга.</p>
    <p>Пожалуйста, проверьте логи и состояние системы.</p>
    """,
    trigger_rule='one_failed',
    dag=dag
)

# Определение зависимостей
check_data_availability >> data_quality_monitor
data_quality_monitor >> performance_monitor
performance_monitor >> external_services_check
external_services_check >> send_critical_alerts
