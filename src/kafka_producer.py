"""
Kafka Producer для отправки данных о полетах
"""
import json
import asyncio
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import structlog

from config.settings import settings
from config.logging import get_logger

logger = get_logger(__name__)


class FlightDataProducer:
    """Kafka Producer для данных о полетах"""

    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self._setup_producer()

    def _setup_producer(self) -> None:
        """Настройка Kafka Producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000,
                max_block_ms=5000,
                request_timeout_ms=30000
            )
            logger.info("Kafka Producer настроен успешно")
        except Exception as e:
            logger.error(f"Ошибка настройки Kafka Producer: {e}")
            raise

    def send_flight_data(self, flight_data: Dict[str, Any], topic: str = None) -> bool:
        """Отправка данных о полете"""
        if not self.producer:
            logger.error("Producer не инициализирован")
            return False

        try:
            topic = topic or settings.kafka_topic_raw_flights
            key = flight_data.get('icao24', 'unknown')

            future = self.producer.send(
                topic,
                value=flight_data,
                key=key
            )

            # Ждем подтверждения
            record_metadata = future.get(timeout=10)
            logger.debug(f"Данные отправлены в топик {record_metadata.topic}, партиция {record_metadata.partition}")
            return True

        except KafkaError as e:
            logger.error(f"Ошибка Kafka при отправке данных: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке данных: {e}")
            return False

    def send_batch_flights(self, flights: list[Dict[str, Any]], topic: str = None) -> int:
        """Отправка пакета данных о полетах"""
        if not flights:
            return 0

        topic = topic or settings.kafka_topic_raw_flights
        success_count = 0

        for flight in flights:
            if self.send_flight_data(flight, topic):
                success_count += 1

        logger.info(f"Отправлено {success_count} из {len(flights)} рейсов")
        return success_count

    def flush(self) -> None:
        """Принудительная отправка всех сообщений"""
        if self.producer:
            self.producer.flush()
            logger.debug("Все сообщения отправлены")

    def close(self) -> None:
        """Закрытие producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka Producer закрыт")


class FlightDataConsumer:
    """Kafka Consumer для данных о полетах"""

    def __init__(self, topic: str = None, group_id: str = "opensky_consumer"):
        self.topic = topic or settings.kafka_topic_raw_flights
        self.group_id = group_id
        self.consumer = None
        self._setup_consumer()

    def _setup_consumer(self) -> None:
        """Настройка Kafka Consumer"""
        try:
            from kafka import KafkaConsumer

            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000
            )
            logger.info(f"Kafka Consumer настроен для топика {self.topic}")
        except Exception as e:
            logger.error(f"Ошибка настройки Kafka Consumer: {e}")
            raise

    def consume_flights(self, max_records: int = None) -> list[Dict[str, Any]]:
        """Получение данных о полетах"""
        if not self.consumer:
            logger.error("Consumer не инициализирован")
            return []

        flights = []
        try:
            message_pack = self.consumer.poll(timeout_ms=1000, max_records=max_records)

            for topic_partition, messages in message_pack.items():
                for message in messages:
                    flight_data = message.value
                    flights.append(flight_data)
                    logger.debug(f"Получен рейс: {flight_data.get('icao24', 'unknown')}")

            logger.info(f"Получено {len(flights)} рейсов")
            return flights

        except Exception as e:
            logger.error(f"Ошибка при получении данных: {e}")
            return []

    def close(self) -> None:
        """Закрытие consumer"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka Consumer закрыт")


# Пример использования
async def main():
    """Пример использования producer и consumer"""
    producer = FlightDataProducer()

    # Пример данных о полете
    sample_flight = {
        "icao24": "abc123",
        "callsign": "TEST123",
        "origin_country": "US",
        "longitude": -74.0,
        "latitude": 40.7,
        "baro_altitude": 10000,
        "velocity": 500,
        "fetch_time": "2024-01-01T12:00:00Z"
    }

    # Отправляем данные
    success = producer.send_flight_data(sample_flight)
    if success:
        logger.info("Данные отправлены успешно")

    producer.close()


if __name__ == "__main__":
    asyncio.run(main())
