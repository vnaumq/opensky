import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os
from dotenv import load_dotenv
import config

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

print("Connecting to Kafka....")

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Connected to Kafka")

print("\n" + "="*50)
flights = config.get_opensky_data(config.get_authorization_token(CLIENT_ID, CLIENT_SECRET))
try:
    while True:
        for flight in flights[:100]:
            if flight:
                message = {
                    "icao24": flight[0],
                    "callsign": flight[1],
                    "origin_country": flight[2],
                    "time_position": flight[3],
                    "last_contact": flight[4],
                    "longitude": flight[5],
                    "latitude": flight[6],
                    "baro_altitude": flight[7],
                    "on_ground": flight[8],
                    "velocity": flight[9],
                    "true_track": flight[10],
                    "vertical_rate": flight[11],
                    "sensors": flight[12],
                    "geo_altitude": flight[13],
                    "squawk": flight[14],
                    "spi": flight[15],
                    "position_source": flight[16],
                    # "category": flight[17]
                }

                producer.send('opensky', value=message)
                print(f"SEND: {message['callsign']} ({message['icao24']})")

        time.sleep(10)

except KeyboardInterrupt:
    print('STOPPING....')
finally:
    producer.close()
    print('STOPPED')
