import requests
import pandas as pd

# OpenSky API endpoint for all states
url = "https://opensky-network.org/api/states/all"
params = {
    "lamin": 35,  # Minimum latitude (Europe)
    "lomin": -10,  # Minimum longitude
    "lamax": 70,  # Maximum latitude
    "lomax": 40   # Maximum longitude
}

# Make the API call
try:
    response = requests.get(url, params=params)
    response.raise_for_status()  # Check for HTTP errors
    data = response.json()['states']
    if data:
        df = pd.DataFrame(data, columns=[
            'icao24', 'callsign', 'origin_country', 'time_position', 'last_contact',
            'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity',
            'true_track', 'vertical_rate', 'sensors', 'geo_altitude', 'squawk',
            'spi', 'position_source'
        ])
        df.to_csv('flights_data.csv', index=False)
        print(df.head())
    else:
        print("No data returned from OpenSky API.")
except requests.RequestException as e:
    print(f"Error fetching data: {e}")