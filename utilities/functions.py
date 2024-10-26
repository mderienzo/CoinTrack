from datetime import datetime, timedelta
import pandas as pd
import time
import yfinance
# modulo_1.py
import logging
from logging_config import setup_logging
from typing import List, Union
import copy
import json

# Configura il logging
setup_logging()

logger = logging.getLogger(__name__)


def divide_year(data_inizio="2022-01-01", data_fine="2022-12-31", n_divisioni=5, milliseconds=False):
    start_date = datetime.strptime(data_inizio, "%Y-%m-%d")
    end_date = datetime.strptime(data_fine, "%Y-%m-%d")
    year_duration = end_date - start_date

    part_duration = year_duration // n_divisioni

    start_time_list = []
    end_time_list = []

    for i in range(n_divisioni):
        start_time = start_date + i * part_duration
        end_time = start_time + part_duration - timedelta(days=1)

        # Elimina la parte delle ore e dei minuti
        start_time = start_time.strftime("%Y-%m-%d")
        end_time = end_time.strftime("%Y-%m-%d")

        start_time = to_unix_timestamp(start_time, milliseconds=milliseconds)
        end_time = to_unix_timestamp(end_time, milliseconds=milliseconds)

        start_time_list.append(start_time)
        end_time_list.append(end_time)

    end_time_list[-1] = to_unix_timestamp(data_fine)
    return start_time_list, end_time_list


def to_unix_timestamp(date_str, milliseconds=False):
    # Converte la data in formato stringa in un oggetto datetime
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    # Calcola il timestamp in secondi (differenza tra la data e l'epoca Unix)
    timestamp = int(date_obj.timestamp())
    if milliseconds:
        # Converte il timestamp in millisecondi
        timestamp = timestamp * 1000

    return timestamp


def get_server_time_difference(client):
    server_time = client.get_server_time()
    server_time = server_time['serverTime'] / 1000.0
    system_time = time.time()
    return system_time - server_time


def unix_to_normal(timestamps: Union[int, List[int], pd.Series], milliseconds: bool = False) -> Union[List[str], pd.Series]:
    """
    Converte una lista di timestamp Unix, un singolo timestamp o una colonna di un dataframe
    in datetime leggibile in formato 'YYYY-MM-DD'.

    :param timestamps: Può essere un singolo timestamp Unix, una lista di timestamp Unix o una colonna di un dataframe.
    :param milliseconds: Se True, i timestamp Unix sono in millisecondi.
    :return: Lista di stringhe o colonna di dataframe con datetime in formato 'YYYY-MM-DD'.
    """
    if isinstance(timestamps, pd.Series):
        if milliseconds:
            return pd.to_datetime(timestamps, unit='ms').dt.strftime('%Y-%m-%d')
        else:
            return pd.to_datetime(timestamps, unit='s').dt.strftime('%Y-%m-%d')

    if not isinstance(timestamps, list):
        timestamps = [timestamps]

    if milliseconds:
        timestamps = [ts / 1000 for ts in timestamps]

    return [datetime.fromtimestamp(ts).strftime('%Y-%m-%d') for ts in timestamps]


def concat_dataframes(*args):
    df_union = pd.DataFrame()
    for csv in args:
        df_union = pd.concat([df_union, pd.read_csv(csv)], ignore_index=True)
    return df_union


def get_coin_price_yahoo(currency, start_time, end_time, interval):
    # Converting timestamp to datetime object if it is a string
    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    logger.debug(f"Moneta: {currency}")
    logger.debug(f"Start_time: {start_time}")
    logger.debug(f"End_time: {end_time}")

    connected = False
    while not connected:
        try:
            data = yfinance.download(tickers=currency, start=start_time, end=end_time, interval=interval)
            time.sleep(0.5)
            connected = True
            logger.info('Connected to yahoo')

            return data

        except Exception as e:
            logger.error("type error: " + str(e))
            return None


def get_exchange_from_to_currency(from_currency, to_currency):
    currency_pair = f"{from_currency}{to_currency}=X"
    connected = False
    while not connected:
        try:
            data = yfinance.download(tickers=currency_pair, interval="1d")
            data[f'{from_currency}_{to_currency}'] = data[["Open", "High", "Low", "Close", "Adj Close"]].mean(axis=1)
            data.reset_index(inplace=True)
            data = data[["Date", f'{from_currency}_{to_currency}']]

            return data

        except Exception as e:
            logger.error("type error: " + str(e))
            return None


def convert_to_unix_ms(dt):
    return int(dt.timestamp() * 1000)


def get_coin_price_binance(symbol, timestamp):
    interval = '1m'
    start = timestamp
    end = timestamp + timedelta(minutes=1)

    start_unix = convert_to_unix_ms(start)
    end_unix = convert_to_unix_ms(end)

    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=1&startTime={start_unix}&endTime={end_unix}'
    try:
        df = pd.read_json(url)
        df.columns = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Closetime',
                      'Quote asset volume', 'Number of trades', 'Taker by base', 'Taker buy quote', 'Ignore']

        close_price = df['Close'][0]

        # Introduci una pausa di 0,1 secondi tra le chiamate API
        time.sleep(0.1)
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        logger.error(f"Request URL: {url}")
        close_price = None

    return close_price


def get_coin_price_kraken(kraken, asset, amount, timestamp):
    if asset == "K":
        asset = "KSM"
    elif asset == "D":
        asset = "DOT"

    time.sleep(5)
    pair = f"{asset}EUR"
    interval = 1440  # Intervallo di 1 giorno in minuti
    dt = datetime.fromtimestamp(timestamp)  # Converte il timestamp in un oggetto datetime
    since = int((dt - timedelta(days=1)).timestamp())  # Data di inizio per i dati OHLC

    ohlc_response = kraken.query_public('OHLC', {'pair': pair, 'interval': interval, 'since': since})
    if ohlc_response['error']:
        logger.error(f"Errore nel recupero dei dati OHLC per {pair}:", ohlc_response['error'])
        return None

    ohlc_data = ohlc_response['result'][pair]
    for data in ohlc_data:
        data_timestamp = int(data[0])
        if data_timestamp > timestamp:
            close_price = float(data[4])
            eur_value = close_price * amount
            return eur_value

    return None


def get_last_day_timestamp(year_str):
    # Converti l'anno da stringa a intero
    year = int(year_str)
    # Ottieni l'ultimo giorno dell'anno
    last_day_of_year = datetime(year, 12, 31, 23, 59, 59)

    return last_day_of_year


def cumulate_amount_and_price(anni_da_dichiarare, dict_giacenze, totale_monete, finestra_temporale_yahoo=5, calculate_price=True):
    tmp_amount = 0.0
    for moneta in totale_monete:
        for anno in anni_da_dichiarare:
            if anno != anni_da_dichiarare[0]:
                dict_giacenze[moneta][anno]["amount"] += tmp_amount
            tmp_amount = dict_giacenze[moneta][anno]["amount"]
            if calculate_price:
                timestamp = get_last_day_timestamp(anno)
                # Calculate start_time and end_time
                end_time = timestamp
                start_time = end_time - timedelta(days=finestra_temporale_yahoo)

                if end_time > datetime.today():
                    end_time = datetime.today().replace(microsecond=0)
                    start_time = end_time - timedelta(days=finestra_temporale_yahoo)

                if dict_giacenze[moneta][anno]['amount'] > 0.0:
                    coppia_moneta_eur = f"{moneta}-EUR"
                    price_df = get_coin_price_yahoo(currency=coppia_moneta_eur,
                                                    start_time=start_time,
                                                    end_time=end_time,
                                                    interval='1d')
                    if not price_df.empty:
                        price_df['mean'] = price_df[["Open", "High", "Low", "Close"]].mean(axis=1).round(decimals=2)
                        price_df.reset_index(inplace=True)
                        max_date_price = price_df.loc[price_df.index.max()]['mean']
                        dict_giacenze[moneta][anno]['amount_eur'] = dict_giacenze[moneta][anno]['amount'] * max_date_price
    return dict_giacenze


def appiattisci_dizionario(dizionario):
    """
    Appiattisce un dizionario di dizionari, portando tutte le chiavi innestate al livello superiore.

    :param dizionario: Dizionario da appiattire.
    :return: Dizionario appiattito.
    """

    def _appiattisci(d, chiavi_precedenti=None):
        if chiavi_precedenti is None:
            chiavi_precedenti = []
        for k, v in d.items():
            nuova_chiave = '.'.join(chiavi_precedenti + [k])
            if isinstance(v, dict):
                _appiattisci(v, chiavi_precedenti + [k])
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        _appiattisci(item, chiavi_precedenti + [f"{k}[{i}]"])
                    else:
                        risultato[f"{nuova_chiave}[{i}]"] = item
            else:
                if nuova_chiave not in risultato or risultato[nuova_chiave] != v:
                    risultato[nuova_chiave] = v

    risultato = {}
    _appiattisci(dizionario)
    return risultato


def crea_dataframe_da_dizionario(chiavi, dizionari):
    """
    Crea un dataframe Pandas con le colonne specificate da una lista di chiavi
    e popola il dataframe con i valori corrispondenti dai dizionari in input.

    :param chiavi: Lista di chiavi che devono diventare colonne nel dataframe.
    :param dizionari: Lista di dizionari da cui estrarre i valori.
    :return: DataFrame Pandas con i valori estratti.
    """
    # Inizializza una lista per contenere i dati
    dati = []

    # Itera su ogni dizionario nella lista di dizionari
    for dizionario in dizionari:
        # Crea un dizionario per la riga corrente
        riga = {}
        for chiave in chiavi:
            try:
                # Tenta di ottenere il valore per la chiave corrente
                riga[chiave] = dizionario[chiave]
            except KeyError:
                # Se la chiave non è presente, inserisci None
                riga[chiave] = None
        # Aggiungi la riga ai dati
        dati.append(riga)

    # Crea il dataframe dai dati
    df = pd.DataFrame(dati, columns=chiavi)

    return df


def handle_list(action, filename, data=None):
    """
    Gestisce una lista scrivendo o caricando da un file.

    Parameters:
    action (str): 'write' per scrivere una lista su un file, 'load' per caricare una lista da un file.
    filename (str): Il nome del file su cui scrivere o da cui caricare la lista.
    data (list, optional): La lista da scrivere su file. Necessaria solo se l'azione è 'write'.

    Returns:
    list: La lista caricata dal file se l'azione è 'load'.
    """
    if action == 'write':
        if data is None:
            raise ValueError("Data must be provided when action is 'write'.")
        with open(filename, 'w') as f:
            for item in data:
                f.write(f"{item}\n")
        print(f"List written to {filename}.")
    elif action == 'load':
        with open(filename, 'r') as f:
            loaded_data = [line.strip() for line in f]
        print(f"List loaded from {filename}.")
        return loaded_data
    else:
        raise ValueError("Invalid action. Use 'write' or 'load'.")
