"""
Модуль мониторинга производительности системы OpenSky
"""
import psutil
import time
from typing import Dict, Any
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import structlog

from config.logging import get_logger

logger = get_logger(__name__)

# Prometheus метрики
cpu_usage_gauge = Gauge('system_cpu_usage_percent', 'CPU usage percentage')
memory_usage_gauge = Gauge('system_memory_usage_percent', 'Memory usage percentage')
disk_usage_gauge = Gauge('system_disk_usage_percent', 'Disk usage percentage')
network_io_counter = Counter('system_network_io_bytes', 'Network I/O bytes', ['direction'])
process_count_gauge = Gauge('system_process_count', 'Number of running processes')

# Метрики приложения
flight_data_counter = Counter('opensky_flights_processed_total', 'Total flights processed')
data_quality_gauge = Gauge('opensky_data_quality_score', 'Data quality score (0-100)')
processing_latency_histogram = Histogram('opensky_processing_latency_seconds', 'Processing latency')


class PerformanceMonitor:
    """Монитор производительности системы"""
    
    def __init__(self, prometheus_port: int = 9090):
        self.prometheus_port = prometheus_port
        self.start_prometheus_server()
    
    def start_prometheus_server(self) -> None:
        """Запуск Prometheus сервера"""
        try:
            start_http_server(self.prometheus_port)
            logger.info(f"Prometheus сервер запущен на порту {self.prometheus_port}")
        except Exception as e:
            logger.error(f"Ошибка запуска Prometheus сервера: {e}")
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Получение системных метрик"""
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_usage_gauge.set(cpu_percent)
            
            # Память
            memory = psutil.virtual_memory()
            memory_usage_gauge.set(memory.percent)
            
            # Диск
            disk = psutil.disk_usage('/')
            disk_usage_gauge.set(disk.percent)
            
            # Сетевой трафик
            network_io = psutil.net_io_counters()
            network_io_counter.labels(direction='sent').inc(network_io.bytes_sent)
            network_io_counter.labels(direction='received').inc(network_io.bytes_recv)
            
            # Количество процессов
            process_count = len(psutil.pids())
            process_count_gauge.set(process_count)
            
            metrics = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'disk_percent': disk.percent,
                'disk_free_gb': disk.free / (1024**3),
                'network_sent_mb': network_io.bytes_sent / (1024**2),
                'network_received_mb': network_io.bytes_recv / (1024**2),
                'process_count': process_count,
                'timestamp': time.time()
            }
            
            logger.debug(f"Системные метрики: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Ошибка получения системных метрик: {e}")
            return {}
    
    def get_application_metrics(self) -> Dict[str, Any]:
        """Получение метрик приложения"""
        try:
            # Здесь можно добавить специфичные метрики приложения
            # Например, количество обработанных рейсов, качество данных и т.д.
            
            metrics = {
                'flights_processed': flight_data_counter._value.get(),
                'data_quality_score': data_quality_gauge._value.get(),
                'avg_processing_latency': processing_latency_histogram._sum.get() / max(processing_latency_histogram._count.get(), 1),
                'timestamp': time.time()
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Ошибка получения метрик приложения: {e}")
            return {}
    
    def check_system_health(self) -> Dict[str, Any]:
        """Проверка здоровья системы"""
        try:
            system_metrics = self.get_system_metrics()
            
            # Определяем статус здоровья
            health_status = "healthy"
            alerts = []
            
            # Проверка CPU
            if system_metrics.get('cpu_percent', 0) > 90:
                health_status = "warning"
                alerts.append("High CPU usage")
            
            # Проверка памяти
            if system_metrics.get('memory_percent', 0) > 90:
                health_status = "warning"
                alerts.append("High memory usage")
            
            # Проверка диска
            if system_metrics.get('disk_percent', 0) > 90:
                health_status = "critical"
                alerts.append("High disk usage")
            
            return {
                'status': health_status,
                'alerts': alerts,
                'metrics': system_metrics,
                'timestamp': time.time()
            }
            
        except Exception as e:
            logger.error(f"Ошибка проверки здоровья системы: {e}")
            return {
                'status': 'error',
                'alerts': [f"Health check failed: {e}"],
                'metrics': {},
                'timestamp': time.time()
            }
    
    def record_flight_processed(self) -> None:
        """Запись метрики обработанного рейса"""
        flight_data_counter.inc()
    
    def record_data_quality(self, score: float) -> None:
        """Запись метрики качества данных"""
        data_quality_gauge.set(score)
    
    def record_processing_latency(self, latency_seconds: float) -> None:
        """Запись метрики задержки обработки"""
        processing_latency_histogram.observe(latency_seconds)


def check_system_performance() -> None:
    """Функция для проверки производительности системы"""
    monitor = PerformanceMonitor()
    
    try:
        health = monitor.check_system_health()
        logger.info(f"Статус системы: {health['status']}")
        
        if health['alerts']:
            for alert in health['alerts']:
                logger.warning(f"Предупреждение: {alert}")
        
        return health
        
    except Exception as e:
        logger.error(f"Ошибка проверки производительности: {e}")
        return None


if __name__ == "__main__":
    # Пример использования
    monitor = PerformanceMonitor()
    
    while True:
        health = monitor.check_system_health()
        print(f"System health: {health}")
        time.sleep(60)  # Проверка каждую минуту
