import time
import requests
import urllib.parse
import hashlib
import hmac
import base64
from pprint import pprint
import datetime
import json
import pandas as pd
from utilities.functions import divide_year, unix_to_normal
import warnings
from tqdm import tqdm

# Read Kraken API key and secret stored in environment variables
api_url = "https://api.kraken.com"

api_key = 'your_api_key'
api_sec = 'your_secret_api_key'



def get_kraken_signature(urlpath, data, secret):
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()


# Attaches auth headers and returns results of a POST request
def kraken_request(uri_path, data, api_key, api_sec):
    headers = {}
    headers['API-Key'] = api_key
    # get_kraken_signature() as defined in the 'Authentication' section
    headers['API-Sign'] = get_kraken_signature(uri_path, data, api_sec)
    req = requests.post((api_url + uri_path), headers=headers, data=data)
    return req


if __name__ == "__main__":
    start_time_list, end_time_list = divide_year(data_inizio="2020-01-01", data_fine=str(datetime.date.today()), n_divisioni=100)

    # Giusto per vedere come ha diviso le date
    timestamp_start = unix_to_normal(start_time_list)
    timestamp_end = unix_to_normal(end_time_list)

    df_lista_movimenti = pd.DataFrame()
    with tqdm(total=len(start_time_list), desc="Processing Blocks") as pbar:
        for i in range(len(start_time_list)):

            payload = json.dumps({
                "nonce": 1234567,
                "trades": True,
                "userref": 12343533,
                "start": start_time_list[i],
                "end": end_time_list[i],
                "closetime": "open"
            })
            resp = kraken_request('/0/private/Ledgers', {
                "nonce": str(int(1000 * time.time())),
                "id": "TCJA"
            }, api_key, api_sec)
            try:
                data = resp.json()['result']['ledger'].values()
                tmp = pd.DataFrame(resp.json()['result']['ledger']).T
                df_lista_movimenti = pd.concat([df_lista_movimenti, tmp], ignore_index=True)
            except:
                warnings.warn("La data di analisi non presenta transazioni", UserWarning)

            time.sleep(1)
            pbar.update(1)

        print(f"Data di inizio {timestamp_start[i]} e Data di Fine {timestamp_end[i]}")

    # Convertire i timestamp in un formato datetime leggibile
    df_lista_movimenti['time'] = pd.to_datetime(df_lista_movimenti['time'], unit='s')

    # Formattare la colonna readable_date per rimuovere i microsecondi
    df_lista_movimenti['time'] = df_lista_movimenti['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # pprint(resp.text)
    pprint(resp.json())
