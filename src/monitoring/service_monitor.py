"""
Модуль мониторинга внешних сервисов OpenSky
"""
import asyncio
import aiohttp
import time
from typing import Dict, Any, List
import structlog

from config.settings import settings
from config.logging import get_logger

logger = get_logger(__name__)


class ServiceMonitor:
    """Монитор внешних сервисов"""
    
    def __init__(self):
        self.services = {
            'opensky_api': 'https://api.opensky-network.org/api/states/all',
            'kafka': f'http://{settings.kafka_bootstrap_servers.split(":")[0]}:9092',
            'postgres': f'postgresql://{settings.postgres_user}:{settings.postgres_password}@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}',
            'prometheus': f'http://localhost:{settings.prometheus_port}',
            'grafana': f'http://localhost:{settings.grafana_port}'
        }
    
    async def check_http_service(self, name: str, url: str, timeout: int = 10) -> Dict[str, Any]:
        """Проверка HTTP сервиса"""
        try:
            start_time = time.time()
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    response_time = time.time() - start_time
                    
                    return {
                        'name': name,
                        'url': url,
                        'status': 'healthy' if response.status == 200 else 'unhealthy',
                        'response_time': response_time,
                        'status_code': response.status,
                        'timestamp': time.time()
                    }
                    
        except asyncio.TimeoutError:
            return {
                'name': name,
                'url': url,
                'status': 'timeout',
                'response_time': timeout,
                'status_code': None,
                'timestamp': time.time()
            }
        except Exception as e:
            return {
                'name': name,
                'url': url,
                'status': 'error',
                'response_time': None,
                'status_code': None,
                'error': str(e),
                'timestamp': time.time()
            }
    
    async def check_database_service(self, name: str, connection_string: str) -> Dict[str, Any]:
        """Проверка сервиса базы данных"""
        try:
            start_time = time.time()
            
            # Здесь можно добавить реальную проверку подключения к БД
            # Для примера используем простую HTTP проверку
            import psycopg2
            
            # Парсим connection string
            conn_parts = connection_string.replace('postgresql://', '').split('@')
            if len(conn_parts) == 2:
                user_pass, host_db = conn_parts
                user, password = user_pass.split(':')
                host_port, db = host_db.split('/')
                host, port = host_port.split(':')
                
                conn = psycopg2.connect(
                    host=host,
                    port=int(port),
                    database=db,
                    user=user,
                    password=password,
                    connect_timeout=5
                )
                conn.close()
                
                response_time = time.time() - start_time
                
                return {
                    'name': name,
                    'connection_string': connection_string,
                    'status': 'healthy',
                    'response_time': response_time,
                    'timestamp': time.time()
                }
            else:
                return {
                    'name': name,
                    'connection_string': connection_string,
                    'status': 'error',
                    'error': 'Invalid connection string',
                    'timestamp': time.time()
                }
                
        except Exception as e:
            return {
                'name': name,
                'connection_string': connection_string,
                'status': 'error',
                'error': str(e),
                'timestamp': time.time()
            }
    
    async def check_all_services(self) -> List[Dict[str, Any]]:
        """Проверка всех сервисов"""
        results = []
        
        # Проверяем HTTP сервисы
        http_tasks = []
        for name, url in self.services.items():
            if name not in ['postgres']:  # PostgreSQL проверяем отдельно
                http_tasks.append(self.check_http_service(name, url))
        
        # Проверяем базу данных
        db_task = self.check_database_service('postgres', self.services['postgres'])
        
        # Выполняем все проверки параллельно
        http_results = await asyncio.gather(*http_tasks, return_exceptions=True)
        db_result = await db_task
        
        # Обрабатываем результаты HTTP сервисов
        for result in http_results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка проверки сервиса: {result}")
            else:
                results.append(result)
        
        # Добавляем результат проверки БД
        results.append(db_result)
        
        return results
    
    def analyze_service_health(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Анализ здоровья сервисов"""
        healthy_count = sum(1 for r in results if r.get('status') == 'healthy')
        total_count = len(results)
        
        unhealthy_services = [r for r in results if r.get('status') != 'healthy']
        
        overall_status = 'healthy' if healthy_count == total_count else 'degraded'
        if healthy_count == 0:
            overall_status = 'critical'
        
        return {
            'overall_status': overall_status,
            'healthy_services': healthy_count,
            'total_services': total_count,
            'health_percentage': (healthy_count / total_count) * 100 if total_count > 0 else 0,
            'unhealthy_services': unhealthy_services,
            'timestamp': time.time()
        }


async def check_external_services() -> Dict[str, Any]:
    """Функция для проверки внешних сервисов"""
    monitor = ServiceMonitor()
    
    try:
        results = await monitor.check_all_services()
        health_analysis = monitor.analyze_service_health(results)
        
        logger.info(f"Проверка сервисов завершена. Статус: {health_analysis['overall_status']}")
        
        if health_analysis['unhealthy_services']:
            for service in health_analysis['unhealthy_services']:
                logger.warning(f"Сервис {service['name']} недоступен: {service.get('error', 'Unknown error')}")
        
        return {
            'service_results': results,
            'health_analysis': health_analysis
        }
        
    except Exception as e:
        logger.error(f"Ошибка проверки внешних сервисов: {e}")
        return {
            'error': str(e),
            'timestamp': time.time()
        }


if __name__ == "__main__":
    # Пример использования
    asyncio.run(check_external_services())
