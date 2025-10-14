import requests
import json
import pandas as pd
from datetime import datetime

# читаем credentials
with open('credentials.json') as f:
    creds = json.load(f)

CLIENT_ID = creds['clientId']
CLIENT_SECRET = creds['clientSecret']

url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

response = requests.post(url, data=data)

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
    df["fetch_time"] = datetime.utcfromtimestamp(data["time"])
    df.to_csv(f"opensky_flights_{data['time']}.csv", index=False)
    print(f"✅ Получено {len(df)} рейсов и сохранено в CSV.")
else:
    print(f"❌ Ошибка: {response.status_code}, {response.text}")
