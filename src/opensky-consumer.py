from kafka import KafkaConsumer
import json

print('CONNECTING TO KAFKA...')

consumer = KafkaConsumer(
    "opensky",
    bootstrap_servers=["kafka:29092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id="group-1"
)

print('CONNECTED TO KAFKA')
print("\n" + "="*50)
print('👁️  WAITING....')
print("="*50 + "\n")

try:
    for message in consumer:
        flight = message.value

        print(f"RECEIVED: {flight.get('callsign')} ({flight.get('icao24')})")

except KeyboardInterrupt:
    print('STOPPING....')
finally:
    consumer.close()
    print("STOPPED")