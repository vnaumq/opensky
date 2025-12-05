import requests


def get_authorization_token(client_id:str, client_secret: str):

    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    response = requests.post(url, data=data)

    if response.status_code == 200:

        return response.json()["access_token"]
    else:

        raise Exception(f"Ошибка: {response.status_code}, {response.text}")


def get_opensky_data(token):
    try:
        headers = {"Authorization": f"Bearer {token}"}

        url = "https://api.opensky-network.org/api/states/all"
        params = {
            'lamin': 35.0,   # минимальная широта
            'lamax': 70.0,   # максимальная широта
            'lomin': -10.0,  # минимальная долгота
            'lomax': 40.0    # максимальная долгота
        }
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        data = response.json()
        states = data.get('states',[])

        print(f'RECEIVED: {len(states)}')

        return states

    except Exception as e:
        print(e)

        return []