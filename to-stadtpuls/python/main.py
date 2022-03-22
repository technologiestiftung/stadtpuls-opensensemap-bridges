import requests
from dotenv import dotenv_values
config = dotenv_values(".env")

SENSEBOX_ID = "5e9af8d545f937001ce58076"
STADTPULS_SENSOR_ID = 79
STADTPULS_TOKEN = config["STADTPULS_TOKEN"]

SENSOR_URL = "https://api.stadtpuls.com/api/v3/sensors/{}/records".format(
    STADTPULS_SENSOR_ID)
HEADERS = {"authorization": "Bearer {}".format(STADTPULS_TOKEN)}


def get_sensor_data(item):
    return float(item['lastMeasurement']['value'])


def main():
    """DOC string"""
    try:
        osm_response = requests.get(
            "https://api.opensensemap.org/boxes/{}?format=json".format(SENSEBOX_ID))
        if osm_response.status_code != 200:
            print("Error while getting data from opensensemap: {}".format(
                osm_response.status_code))
        else:
            data = osm_response.json()
            measurements = map(get_sensor_data, data['sensors'])
            payload = {"measurements": list(measurements)}
            print("GET these values from opensensemap.org", payload)
            try:
                stadtpuls_response = requests.post(
                    SENSOR_URL, json=payload, headers=HEADERS)
                if stadtpuls_response.status_code != 201:
                    print("Error posting data to stadtpuls.com")
                else:
                    print("POSTed these values to stadtpuls.com",
                          stadtpuls_response.json())
            except requests.exceptions.RequestException as error:
                print(error)

    except requests.exceptions.RequestException as error:
        print(error)


main()
