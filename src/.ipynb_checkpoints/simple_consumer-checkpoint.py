from kafka import KafkaConsumer
import json

print('connecting with Kafka....')

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_rese='earlist',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='my-first-group'
)

print('Consummer connected!')

try:
    for message in consumer:
        print("=" * 50)
        print(f"📩 ПОЛУЧЕНО СООБЩЕНИЕ:")
        print(f"   Топик: {message.topic}")
        print(f"   Партиция: {message.partition}")
        print(f"   Смещение (offset): {message.offset}")
        print(f"   Ключ: {message.key}")
        print(f"   Значение: {message.value}")
        print(f"   Время: {message.timestamp}")
        print("=" * 50)
        print()
except KeyboardInterrupt:
    print('Stopped')

consumer.close()

print('Consumer Closed')