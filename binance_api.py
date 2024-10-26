from binance.client import Client
import json
from datetime import datetime, timedelta
import pandas as pd
import time
import numpy as np
from utilities.functions import divide_year, unix_to_normal

class Binance:
    """
    Classe che interagisce con l'API Binance per sincronizzare l'orario e gestire le chiamate API.

    Attributi:
        client (Client): Client Binance per accedere ai metodi dell'API.
        time_offset (int): Differenza temporale tra server Binance e sistema locale.
    """
    
    def __init__(self, public_key='', secret_key='', sync=False):
        """
        Inizializza la classe Binance con le chiavi API e sincronizza l'orario se richiesto.
        
        Args:
            public_key (str): Chiave pubblica Binance.
            secret_key (str): Chiave segreta Binance.
            sync (bool): Sincronizza l'orario se True.
        """
        self.time_offset = 0
        self.client = Client(public_key, secret_key)

        if sync:
            self.time_offset = self.get_time_offset()

    def get_time_offset(self):
        """
        Calcola la differenza di orario tra il server di Binance e l'orario locale.
        
        Returns:
            int: Differenza di orario in millisecondi.
        """
        res = self.client.get_server_time()
        return res['serverTime'] - int(time.time() * 1000)

    def synced(self, fn_name, **args):
        """
        Esegue una funzione sincronizzata con l'orario di Binance.
        
        Args:
            fn_name (str): Nome della funzione da chiamare.
            **args: Argomenti per la funzione dell'API Binance.
        
        Returns:
            Response from Binance API.
        """
        args['timestamp'] = int(time.time() - self.time_offset)
        return getattr(self.client, fn_name)(**args)

def convert_to_unix_ms(dt):
    """
    Converte una data in formato UNIX timestamp in millisecondi.
    
    Args:
        dt (datetime): Data da convertire.
    
    Returns:
        int: Timestamp in millisecondi.
    """
    return int(dt.timestamp() * 1000)

def get_coin_value_at_timestamp(symbol, amount, timestamp):
    """
    Ottiene il valore di una criptovaluta ad un determinato timestamp.
    
    Args:
        symbol (str): Simbolo della criptovaluta (es. 'BTC').
        amount (float): Quantità dell'asset.
        timestamp (datetime): Timestamp della richiesta.

    Returns:
        float or None: Valore di chiusura dell'asset moltiplicato per la quantità, o None in caso di errore.
    """
    interval = '1m'
    start = timestamp
    end = timestamp + timedelta(minutes=1)

    start_unix = convert_to_unix_ms(start)
    end_unix = convert_to_unix_ms(end)

    symbol = symbol + 'USDT'
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=1&startTime={start_unix}&endTime={end_unix}'
    try:
        df = pd.read_json(url)
        df.columns = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Closetime',
                      'Quote asset volume', 'Number of trades', 'Taker by base', 'Taker buy quote', 'Ignore']

        close_price = df['Close'][0] * np.float64(amount)

        # Introduce una pausa tra le chiamate API per evitare limitazioni
        time.sleep(0.1)
    except Exception as e:
        print(f"Error occurred: {e}")
        print(f"Request URL: {url}")
        close_price = None

    return close_price

def pretty_print(input_data):
    """
    Stampa JSON formattato in modo leggibile.
    
    Args:
        input_data (str, dict, list): Dati in formato JSON o stringa JSON.
    """
    if isinstance(input_data, str):
        try:
            json_data = json.loads(input_data)
        except json.JSONDecodeError:
            print("La stringa fornita non è un JSON valido.")
            return
    elif isinstance(input_data, (dict, list)):
        json_data = input_data
    else:
        print("Il tipo di dato fornito non è valido.")
        return

    pretty_json = json.dumps(json_data, indent=4)
    print(pretty_json)

def add_modify_info(data, campo_temporale):
    """
    Modifica e aggiunge informazioni a un dizionario di dati Binance.
    
    Args:
        data (list of dict): Dati Binance.
        campo_temporale (str): Chiave che contiene il timestamp.

    Returns:
        list of dict: Dati modificati con timestamp convertiti e controvalore aggiunto.
    """
    for item in data:
        if campo_temporale in item:
            timestamp = item[campo_temporale] // 1000
            dt_object = datetime.fromtimestamp(timestamp)
            item[campo_temporale] = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            item["controvalore"] = get_coin_value_at_timestamp(item["asset"], item["amount"], dt_object)
        print(item)

    return data

if __name__ == "__main__":
    # Inserire le proprie chiavi Binance
    api_key = "your_api_key"
    secret_key = "your_secret_key"

    # Inizializzazione del client Binance
    binance = Binance(public_key=api_key, secret_key=secret_key, sync=True)

    # Divisione dell'anno per eseguire chiamate parziali ai dati
    start_time_list, end_time_list = divide_year(data_inizio="2023-01-01", data_fine="2023-12-31", n_divisioni=100)

    # Conversione dei timestamp UNIX in formato leggibile
    timestamp_start = unix_to_normal(start_time_list)
    timestamp_end = unix_to_normal(end_time_list)

    # Ciclo per recuperare e modificare i dati di dividendi
    for i in range(len(start_time_list)):
        dividend = binance.synced('get_asset_dividend_history', startTime=start_time_list[0], endTime=end_time_list[0], limit=500)

        print("Total Transactions: {}".format(dividend["total"]))
        print("Period of analysis: {} - {}".format(timestamp_start[i], timestamp_end[i]))

        add_modify_info(dividend["rows"], "divTime")

        if i == 0:
            df_finale = pd.DataFrame(dividend["rows"])
        else:
            df_temp = pd.DataFrame(dividend["rows"])
            df_finale = pd.concat([df_finale, df_temp], ignore_index=True)

        print("Iteration n° {}".format(i))
        print(df_finale)
