import requests
import json
import pandas as pd
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

response = requests.post(url, data=data)
print(response.text)
if response.status_code == 200:
    token = response.json()["access_token"]
    print(f"✅ Access Token:\n{token}")
else:
    print(f"❌ Ошибка: {response.status_code}, {response.text}")

import requests

TOKEN = token
headers = {"Authorization": f"Bearer {TOKEN}"}

url = "https://api.opensky-network.org/api/states/all"

params = {
    "lamin": 35.0,
    "lomin": -10.0,
    "lamax": 60.0,
    "lomax": 30.0
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    cols = [
        "icao24", "callsign", "origin_country", "time_position",
        "last_contact", "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source"
    ]
    df = pd.DataFrame(data["states"], columns=cols)
    df["fetch_time"] = datetime.fromtimestamp(data["time"], tz=timezone.utc)
    # Получаем путь к директории, где находится текущий скрипт
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Создаем полный путь для сохранения файла
    file_name = f"files/opensky_flights_{data['time']}.csv"
    full_path = os.path.join(current_dir, file_name)

    df.to_csv(full_path, index=False)
    print(f"✅ Получено {len(df)} рейсов и сохранено в CSV.")
else:
    print(f"❌ Ошибка: {response.status_code}, {response.text}")
