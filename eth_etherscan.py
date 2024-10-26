from web3 import Web3
from requests import get
from matplotlib import pyplot as plt
from datetime import datetime
from pprint import pprint
from tqdm import tqdm
import pandas as pd
from utilities.functions import cumulate_amount_and_price, crea_dataframe_da_dizionario
import copy


API_KEY = "your_api_key"
BASE_URL = "https://api.etherscan.io/api"
ether_decimal = 18


def make_api_url(module, action, address, **kwargs):
    url = BASE_URL + f"?module={module}&action={action}&address={address}&apikey={API_KEY}"

    for key, value in kwargs.items():
        url += f"&{key}={value}"

    return url

def get_transactions(address):

    transactions_url = make_api_url("account", "txlist", address, startblock=0, endblock=99999999, page=1, offset=10000, sort="asc")
    response = get(transactions_url)
    data = response.json()["result"]

    internal_tx_url = make_api_url("account", "txlistinternal", address, startblock=0, endblock=99999999, page=1, offset=10000, sort="asc")
    response2 = get(internal_tx_url)
    data2 = response2.json()["result"]

    transactions_url = make_api_url("account", "tokentx", address, startblock=0, endblock=99999999, page=1, offset=10000, sort="asc")
    response = get(transactions_url)
    data3 = response.json()["result"]


    data.extend(data2)
    data.extend(data3)
    data.sort(key=lambda x: int(x['timeStamp']))

    return data

def get_clean_dataframe(address):
    data = get_transactions(address)

    tx_eth = []
    for tx in data:
        to_addr = tx["to"]
        from_addr = tx["from"]

        if 'tokenSymbol' not in tx:
            symbol = 'ETH'
            decimal = 10 ** ether_decimal
        else:
            symbol = tx['tokenSymbol']
            decimal = 10 ** int(tx['tokenDecimal'])

        value = int(tx.get("value", '')) / decimal

        if "gasPrice" in tx:
            gas = int(tx["gasUsed"]) * int(tx["gasPrice"]) / 10 ** ether_decimal
        else:
            gas = int(tx["gasUsed"]) / 10 ** ether_decimal

        time = datetime.fromtimestamp(int(tx['timeStamp']))


        direction_in = to_addr.lower() == address.lower()
        direction_out = from_addr.lower() == address.lower()
        if direction_in and not direction_out:
            in_our_out = 'IN'
        elif direction_out and not direction_in:
            in_our_out = 'OUT'
            value -= gas

        tx_cleaned = {
            'data': time,
            'currency': symbol,
            'amount': value,
            'fee': gas,
            'in_or_out': in_our_out,
            'txHash': tx['hash'],
        }
        tx_eth.append(tx_cleaned)
    df = crea_dataframe_da_dizionario(['data', 'currency', 'amount', 'fee', 'in_or_out', 'txHash'], tx_eth)
    df['amount'] = df['amount'].abs()

    return df


    return

def composizione_eth(address, anni_da_dichiarare):

    df_lista_movimenti = get_clean_dataframe(address)

    colonna_data = "data"
    colonna_currency = "currency"
    colonna_amount = "amount"
    colonna_in_or_out = 'in_or_out'
    # Converti la colonna in datetime
    df_lista_movimenti[colonna_data] = pd.to_datetime(df_lista_movimenti[colonna_data])
    totale_monete = df_lista_movimenti[colonna_currency].unique()
    dict_amount = {
        "amount": 0,
        "amount_eur": 0
    }
    dict_year = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}
    dict_rep_fin_per_moneta = {moneta: copy.deepcopy(dict_year) for moneta in totale_monete}
    for anno in anni_da_dichiarare:
        df_mov_per_anno = df_lista_movimenti[df_lista_movimenti[colonna_data].dt.year == anno]
        if not df_mov_per_anno.empty:
            for moneta in totale_monete:
                df_mov_per_anno_moneta = df_mov_per_anno[df_mov_per_anno[colonna_currency] == moneta]
                df_mov_per_anno_moneta_in = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_in_or_out] == 'IN']
                df_mov_per_anno_moneta_out = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_in_or_out] == 'OUT']
                if not df_mov_per_anno_moneta_in.empty:
                    dict_rep_fin_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_in[colonna_amount].sum()
                if not df_mov_per_anno_moneta_out.empty:
                    dict_rep_fin_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_out[colonna_amount].sum()

    dict_rep_fin_per_moneta = cumulate_amount_and_price(anni_da_dichiarare, dict_rep_fin_per_moneta, totale_monete, calculate_price=False)
    return dict_rep_fin_per_moneta




if __name__ == "__main__":

    address = '0x...'


    dict_rep_fin_per_moneta = composizione_eth(address, [2020,2021,2022,2023,2024])
    pprint(dict_rep_fin_per_moneta)