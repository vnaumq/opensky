from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer = lambda x: json.dumps(x).encode('utf-8')
)

print('Продюсер подключен')

message = {
    "text": "Привет, это Маша",
    "number": 1,
    "timestamp": "2024-12-01"

}

producer.send('test-topic', value=message)
print(f"сообщение отправлено: {message}")


for i in range(2,6):
    new_message = {
        "text": f"Message number: {i}"
    }
    producer.send('test-topic', value=new_message)
    print(f'Сообщение отправлено: {new_message}')

producer.flush()
print("all message send")
    
producer.close()
print('conn close')