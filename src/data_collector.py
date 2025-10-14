"""
Модуль для сбора данных о полетах с OpenSky API
"""
import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import aiohttp
import pandas as pd
from kafka import KafkaProducer
import structlog

from config.settings import settings
from config.logging import get_logger

logger = get_logger(__name__)


class OpenSkyDataCollector:
    """Класс для сбора данных о полетах с OpenSky API"""

    def __init__(self):
        self.base_url = "https://api.opensky-network.org/api"
        self.auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        self.token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        self.producer: Optional[KafkaProducer] = None

    async def _get_access_token(self) -> str:
        """Получение токена доступа"""
        if self.token and self.token_expires_at and datetime.now(timezone.utc) < self.token_expires_at:
            return self.token

        auth_data = {
            "grant_type": "client_credentials",
            "client_id": settings.opensky_client_id,
            "client_secret": settings.opensky_client_secret
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.auth_url, data=auth_data) as response:
                if response.status == 200:
                    token_data = await response.json()
                    self.token = token_data["access_token"]
                    # Токен действителен 1 час, обновляем за 5 минут до истечения
                    self.token_expires_at = datetime.now(timezone.utc).replace(
                        minute=datetime.now(timezone.utc).minute + 55
                    )
                    logger.info("Access token получен успешно")
                    return self.token
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка получения токена: {response.status}, {error_text}")
                    raise Exception(f"Не удалось получить токен: {response.status}")

    def _setup_kafka_producer(self) -> None:
        """Настройка Kafka producer"""
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000
            )
            logger.info("Kafka producer настроен")

    async def _fetch_flight_data(self, bounds: Dict[str, float]) -> Optional[Dict[str, Any]]:
        """Получение данных о полетах"""
        token = await self._get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.base_url}/states/all"
        params = {
            "lamin": bounds["lamin"],
            "lomin": bounds["lomin"],
            "lamax": bounds["lamax"],
            "lomax": bounds["lomax"]
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Получено {len(data.get('states', []))} рейсов")
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка API: {response.status}, {error_text}")
                    return None

    def _process_flight_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Обработка и нормализация данных о полетах"""
        if not raw_data or not raw_data.get('states'):
            return []

        columns = [
            "icao24", "callsign", "origin_country", "time_position",
            "last_contact", "longitude", "latitude", "baro_altitude",
            "on_ground", "velocity", "true_track", "vertical_rate",
            "sensors", "geo_altitude", "squawk", "spi", "position_source"
        ]

        processed_flights = []
        fetch_time = datetime.fromtimestamp(raw_data['time'], tz=timezone.utc)

        for state in raw_data['states']:
            if state:  # Проверяем, что state не None
                flight_data = {
                    "fetch_time": fetch_time.isoformat(),
                    "api_time": raw_data['time']
                }

                # Заполняем данные о полете
                for i, col in enumerate(columns):
                    if i < len(state) and state[i] is not None:
                        flight_data[col] = state[i]
                    else:
                        flight_data[col] = None

                processed_flights.append(flight_data)

        return processed_flights

    async def _send_to_kafka(self, flights: List[Dict[str, Any]]) -> None:
        """Отправка данных в Kafka"""
        if not flights:
            return

        self._setup_kafka_producer()

        for flight in flights:
            try:
                # Используем icao24 как ключ для партиционирования
                key = flight.get('icao24', 'unknown')
                self.producer.send(
                    settings.kafka_topic_raw_flights,
                    value=flight,
                    key=key
                )
            except Exception as e:
                logger.error(f"Ошибка отправки в Kafka: {e}")

        self.producer.flush()
        logger.info(f"Отправлено {len(flights)} рейсов в Kafka")

    async def collect_data(self, bounds: Dict[str, float]) -> None:
        """Основной метод сбора данных"""
        try:
            logger.info(f"Начинаем сбор данных для региона: {bounds}")

            # Получаем данные
            raw_data = await self._fetch_flight_data(bounds)
            if not raw_data:
                logger.warning("Данные не получены")
                return

            # Обрабатываем данные
            flights = self._process_flight_data(raw_data)
            if not flights:
                logger.warning("Нет данных для обработки")
                return

            # Отправляем в Kafka
            await self._send_to_kafka(flights)

            logger.info(f"Сбор данных завершен. Обработано {len(flights)} рейсов")

        except Exception as e:
            logger.error(f"Ошибка при сборе данных: {e}")
            raise

    def close(self) -> None:
        """Закрытие соединений"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer закрыт")


async def main():
    """Основная функция"""
    # Регионы для сбора данных
    regions = {
        "europe": {
            "lamin": 35.0,
            "lomin": -10.0,
            "lamax": 70.0,
            "lomax": 40.0
        },
        "north_america": {
            "lamin": 25.0,
            "lomin": -125.0,
            "lamax": 50.0,
            "lomax": -65.0
        }
    }

    collector = OpenSkyDataCollector()

    try:
        # Собираем данные для каждого региона
        for region_name, bounds in regions.items():
            logger.info(f"Сбор данных для региона: {region_name}")
            await collector.collect_data(bounds)
            await asyncio.sleep(1)  # Небольшая пауза между запросами

    finally:
        collector.close()


if __name__ == "__main__":
    asyncio.run(main())
