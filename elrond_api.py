import requests
import pandas as pd
import numpy as np
from decimal import Decimal
import yfinance as yahoo_finance
import copy
from metadata.elrond_metadata_api import add_amount, sub_amount, lista_chiavi, lista_colonne_tx_standard
from utilities.functions import get_exchange_from_to_currency, get_coin_price_yahoo, concat_dataframes, cumulate_amount_and_price, divide_year_per_elrond, unix_to_normal, appiattisci_dizionario, crea_dataframe_da_dizionario, handle_list
from pprint import pprint
from urllib.parse import urlencode
import re
import logging
from logging_config import setup_logging

# Configura il logging
setup_logging()

logger = logging.getLogger(__name__)



def get_transactions(url):
    headers = {
        "accept": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        transactions = response.json()
        if not transactions:
            logger.info("La lista di tranzioni è vuota")
        return transactions
    else:
        print(f"Si è verificato un errore. Codice di stato: {response.status_code}")
        return


def get_coin(row):
    if row['action.arguments.transfers[0].ticker'] != None and row['action.arguments.transfers[0].type'] == 'MetaESDT':
        return 'MEX'
    elif row['action.arguments.transfers[0].ticker'] != None and row['action.arguments.transfers[0].type'] == 'FungibleESDT':
        return row['action.arguments.transfers[0].ticker']
    else:
        return 'EGLD'


def determine_direction_transaction(row, address):
    if row['sender'] == address and row['receiver'] != address:
        return 'OUT'
    elif row['receiver'] == address and row['sender'] != address:
        return 'IN'
    else:
        return 'OTHER'


def clean_operation_type(row):
    if row['type'] == 'nft' or row['type'] == 'esdt':
        return row['ticker']
    else:
        return row['type']


def clean_function(row):
    if row['action.name'] != None and row['action.name'] != '':
        return row['action.name']
    else:
        return row['function']


def handle_case(case):
    switcher = {
        'LKFARM': "MEX14899",
        'MEXFARM': "MEX14899",
        'MEXFARML': "MEX14899",
        'XMEX': 'MEX14899',
        'LKMEX': "MEX14899",
        'MEX': "MEX14899",
        'SRIDE': "RIDE",
        'SZPAY': "ZPAY",
        'SITHEUM': "ITHEUM",
        'SBHAT': "BHAT",
    }
    # Ottieni la funzione corrispondente dalla mappa del dizionario
    func = switcher.get(case, lambda: "Invalid")
    try:
        return func()
    except:
        return func


def get_coin_value(row):
    try:
        if row["coin"] != None:
            obtain_corrispondent_coin = handle_case(row["coin"])
            if obtain_corrispondent_coin == "Invalid":
                coin = row["coin"] + "-" + "USD"
            else:
                coin = obtain_corrispondent_coin + "-" + "USD"
            value = get_coin_price_yahoo(coin, row["readable_timestamp"], "1d")

            return value
        else:
            return None
    except:
        return None


def calculate_price(df):
    """
    Function useful for calculate the price 
    """

    from_currency = "USD"
    to_currency = "EUR"
    exchange = get_exchange_from_to_currency(from_currency, to_currency)
    exchange.sort_values("Date", inplace=True)  # Assicurati che 'exchange' sia ordinato per 'Date'
    # Aggiunge la colonna del cambio
    df["prezzo_usd"] = df.apply(get_coin_value, axis=1)
    # Esegui un merge_asof
    merged_df = pd.merge_asof(df, exchange, left_on="readable_timestamp", right_on="Date")
    merged_df["prezzo_eur"] = merged_df["prezzo_usd"] * merged_df["usd_eur"]
    merged_df["controvalore_eur"] = merged_df["prezzo_eur"] * merged_df["amount"].astype(np.float64)
    columns_to_keep = ["coin", "controvalore_eur", "function", "amount", "prezzo_eur", "prezzo_usd", "type", "readable_timestamp", "receiver/sender - Assets", "sender", "receiver", "txHash"]
    merged_df = merged_df[columns_to_keep]
    merged_df.sort_values("readable_timestamp", ascending=False, inplace=True)
    print(merged_df)
    return merged_df


def estrazione_lista_tx_with_operations_in_out(lista_transazioni_appiattite, address):

    lista_tx_with_op = []
    for tx in lista_transazioni_appiattite:


        # La prima tx da salvare sarà quella che ha i valori standard.
        timestamp = tx.get('timestamp', '')
        action_name = tx.get('action.name', '')
        # currency = tx.get('type', '')
        fee = tx.get('fee', '')
        function = tx.get('function', '')
        currency = 'egld' if function == 'transfer' or function == 'wrapEgld' or action_name == 'confirmTickets' or action_name == 'withdraw' or action_name == 'buyTickets' and tx.get('type') == None else None
        value = tx.get('value', '')
        sender = tx.get('sender', '')
        receiver = tx.get('receiver', '')
        ticker = tx.get('ticker', '')
        txHash = tx.get('txHash', '')


        dizionario_tx = dict(zip(lista_colonne_tx_standard, [timestamp, action_name, currency, fee,
                                                             function, value, sender, receiver, ticker, txHash]))

        lista_tx_with_op.append(dizionario_tx)

        add_details_to_tx(address, lista_tx_with_op, timestamp, tx, txHash, type_detail = 'operations')

    return lista_tx_with_op


def add_details_to_tx(address, lista_tx_with_op, timestamp, tx, txHash, type_detail):
    # Ora procedo nel capire se la stessa transazione aveva anche delle chiavi del dizionario con le operation
    # Se non li ha vado avanti
    total_keys = tx.keys()
    function_origin_tx = tx.get('function', '')
    all_operations_key = [key for key in total_keys if key.startswith(type_detail)]
    if all_operations_key:
        # Espressione regolare per trovare gli indici
        pattern = re.compile(fr'{type_detail}\[(\d+)\]')
        # Estrarre tutti gli indici dalla lista di stringhe
        indices = [int(match.group(1)) for string in all_operations_key for match in pattern.finditer(string)]

        # Trovare il primo e l'ultimo indice
        first_index = min(indices)
        last_index = max(indices)

        action_name = ''
        fee = 0


        # Scandagliare tra le operazioni
        contatore = 0
        for index in range(first_index, last_index + 1):
            type_key = f'{type_detail}[{index}].type'
            function_key = f'{type_detail}[{index}].action'
            value_key = f'{type_detail}[{index}].value'
            sender_key = f'{type_detail}[{index}].sender'
            receiver_key = f'{type_detail}[{index}].receiver'
            ticker_key = f'{type_detail}[{index}].ticker'

            type = tx.get(type_key, '')
            if type_detail == 'results':
                function = 'smart_contract_result'
            elif type_detail == 'operations' and function_origin_tx not in ['unBond', 'withdraw']:
                function = tx.get(function_key, '')
            elif function_origin_tx in ['unBond', 'withdraw']:
                function = function_origin_tx
            value = tx.get(value_key, '')
            sender = tx.get(sender_key, '')
            receiver = tx.get(receiver_key, '')
            ticker = tx.get(ticker_key, '')

            if sender == address and receiver != address and type != 'log':
                dizionario_tx = dict(zip(lista_colonne_tx_standard, [timestamp, action_name, type, fee,
                                                                     function, value, sender, receiver, ticker, txHash]))

                lista_tx_with_op.append(dizionario_tx)

            elif receiver == address and sender != address and type != 'log':
                dizionario_tx = dict(zip(lista_colonne_tx_standard, [timestamp, action_name, type, fee,
                                                                     function, value, sender, receiver, ticker, txHash]))

                lista_tx_with_op.append(dizionario_tx)
    return

def get_clean_dataframe(anni_da_dichiarare, lista_transazioni, address):
    base_url = f'https://api.multiversx.com/accounts/{address}/transactions'

    for anno in anni_da_dichiarare:
        first_day_of_the_year = f"{anno}-01-01"
        last_day_of_the_year = f"{anno}-12-31"
        first_day_list, last_day_list = divide_year_per_elrond(data_inizio=first_day_of_the_year, data_fine=last_day_of_the_year, n_divisioni=10, milliseconds=False)
        for i in range(len(first_day_list)):

            params = {
                'status': 'success',
                'after': f'{first_day_list[i]}',
                'before': f'{last_day_list[i]}',
                'order': 'asc',
                'withScResults': 'true',
                'withOperations': 'true',
                'withLogs': 'true',
                'withBlockInfo': 'true',
                'withActionTransferValue': 'true'
            }

            url = f'{base_url}?{urlencode(params)}'
            logger.info(f"Inizio: {unix_to_normal(first_day_list[i])} , Fine: {unix_to_normal(last_day_list[i])}")
            transactions = get_transactions(url)
            if transactions:
                lista_transazioni.append(transactions)
    lista_transazioni_appiattite = []
    for lista_trans in lista_transazioni:
        for tx in lista_trans:
            lista_transazioni_appiattite.append(appiattisci_dizionario(tx))

    # lista_transazioni_appiattite = handle_list('load', 'transactions.txt', data=None)
    lista_tx_with_op = estrazione_lista_tx_with_operations_in_out(lista_transazioni_appiattite, address)

    df = crea_dataframe_da_dizionario(lista_colonne_tx_standard, lista_tx_with_op)

    # Gestisci il formato unix in formato date
    df['timestamp'] = unix_to_normal(df['timestamp'])
    # Converte la colonna 'fee' e 'value' in Decimal e scala i valori
    df['fee'] = df['fee'].apply(lambda x: Decimal(x) / (10 ** 18))
    df['value'].replace('', 0, inplace=True)
    df['value'] = df['value'].apply(lambda x: Decimal(x) / (10 ** 18))

    df['IN_OR_OUT'] = df.apply(determine_direction_transaction, axis=1, address=address)

    df['type'] = df.apply(clean_operation_type, axis=1)

    df['function'] = df.apply(clean_function, axis=1)
    df.drop(['action.name', 'ticker'], axis = 1, inplace = True)
    return df


def composizione_elrond_api(anni_da_dichiarare, address):
    lista_transazioni = []

    df_lista_movimenti = get_clean_dataframe(anni_da_dichiarare, lista_transazioni, address)

    # Aggiunta colonna della currency

    colonna_data = "timestamp"
    colonna_currency = "type"
    colonna_amount = "value"
    colonna_tipo_azione = "function"
    colonna_fee = "fee"
    # Converti la colonna in datetime
    df_lista_movimenti[colonna_data] = pd.to_datetime(df_lista_movimenti[colonna_data])
    df_lista_movimenti.sort_values(colonna_data, inplace=True)
    totale_monete = df_lista_movimenti[colonna_currency].unique()
    totale_monete = [moneta for moneta in totale_monete if moneta != None]
    totale_azioni = df_lista_movimenti[colonna_tipo_azione].unique()

    campi_da_calcolare = ["amount", "amount_eur"]
    dict_amount = {campo: 0 for campo in campi_da_calcolare}
    dict_year = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}
    dict_giacenze_per_moneta = {moneta: copy.deepcopy(dict_year) for moneta in totale_monete}

    for anno in anni_da_dichiarare:
        df_mov_per_anno = df_lista_movimenti[df_lista_movimenti[colonna_data].dt.year == anno]
        if not df_mov_per_anno.empty:
            for moneta in totale_monete:
                df_mov_per_anno_moneta = df_mov_per_anno[df_mov_per_anno[colonna_currency] == moneta]
                if moneta == 'egld':
                    dict_giacenze_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno[colonna_fee].sum()
                for azione in totale_azioni:
                    df_mov_per_anno_moneta_azione = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_tipo_azione] == azione]
                    if not df_mov_per_anno_moneta_azione.empty:
                        df_mov_per_anno_moneta_azione_in = df_mov_per_anno_moneta_azione[df_mov_per_anno_moneta_azione['IN_OR_OUT'] == 'IN']
                        df_mov_per_anno_moneta_azione_out = df_mov_per_anno_moneta_azione[df_mov_per_anno_moneta_azione['IN_OR_OUT'] == 'OUT']
                        if azione == 'transfer' or azione == 'wrapEgld':
                            dict_giacenze_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione_in[colonna_amount].sum()
                            dict_giacenze_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_azione_out[colonna_amount].sum()

                        if azione in ['confirmTickets', 'buyTickets']:
                            dict_giacenze_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_azione_out[colonna_amount].sum()
    dict_giacenze_per_moneta = cumulate_amount_and_price(anni_da_dichiarare, dict_giacenze_per_moneta, totale_monete, calculate_price=False)

    # COSTRUZIONE DELLA VISTA DELLE GIACENZE PER ANNO
    dict_per_moneta = {moneta: 0 for moneta in totale_monete}
    dict_amount = {campo: copy.deepcopy(dict_per_moneta) for campo in campi_da_calcolare}
    dict_giacenze_per_anno = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}

    for anno in anni_da_dichiarare:
        for moneta in totale_monete:
            dict_giacenze_per_anno[anno]["amount"][moneta] = dict_giacenze_per_moneta[moneta][anno]["amount"]
            dict_giacenze_per_anno[anno]["amount_eur"][moneta] = dict_giacenze_per_moneta[moneta][anno]["amount_eur"]

    return dict_giacenze_per_moneta


if __name__ == "__main__":

    address = '...'


    anni_da_dichiarare = [2020, 2021, 2022, 2023, 2024]
    dict_giacenze_per_moneta = composizione_elrond_api(anni_da_dichiarare, address)

    pprint(dict_giacenze_per_moneta, width=60)

