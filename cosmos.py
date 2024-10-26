from requests import get
from datetime import datetime
from pprint import pprint
from tqdm import tqdm
import pandas as pd
from utilities.functions import cumulate_amount_and_price, crea_dataframe_da_dizionario, unix_to_normal
import copy

# URL for the RPC node connection
rpc_url = 'https://cosmos-rpc.publicnode.com/'  # Example of a public node
API_KEY = '...'  # Insert your API key here

def get_transaction(address):
    """
    Fetches transaction data for a given address from the OKLink API.

    Args:
        address (str): The blockchain address for which transaction data is to be fetched.
    
    Returns:
        list: A list of transactions associated with the address.
    """
    import requests
    base_url = 'https://www.oklink.com'

    url = f'{base_url}/api/v5/explorer/address/normal-transaction-cosmos?chainShortName=cosmos&address={address}&limit=100'

    # Set the headers with API key
    headers = {
        'Ok-Access-Key': API_KEY
    }
    
    # Fetch response and return transaction data
    response = requests.get(url, headers=headers)
    data = response.json()['data']['transactionList']
    return data

def get_clean_dataframe(address):
    """
    Cleans and structures transaction data into a DataFrame for processing.

    Args:
        address (str): The blockchain address for transaction data.
    
    Returns:
        pd.DataFrame: Cleaned DataFrame containing transaction information.
    """
    data = get_transaction(address)
    tx_cosmos = []
    
    for tx in data:
        value = float(tx['value'])
        fee = float(tx['txFee'])
        to_addr = tx['to']
        from_addr = tx['from']
        symbol = tx['symbol']
        time = unix_to_normal(int(tx['transactionTime']))[0]

        # Determine if the transaction is incoming or outgoing
        direction_in = address in to_addr
        direction_out = address in from_addr
        if direction_in and not direction_out:
            in_or_out = 'IN'
        elif direction_out and not direction_in:
            in_or_out = 'OUT'
        else:
            in_or_out = 'OTHER'

        # Structure transaction data
        tx_cleaned = {
            'data': time,
            'currency': symbol,
            'amount': value,
            'fee': fee,
            'in_or_out': in_or_out,
            'txHash': tx['txId'],
        }
        tx_cosmos.append(tx_cleaned)
    
    # Create DataFrame from the structured data
    df = crea_dataframe_da_dizionario(['data', 'currency', 'amount', 'fee', 'in_or_out', 'txHash'], tx_cosmos)
    return df

def composizione_cosmos(address, anni_da_dichiarare):
    """
    Aggregates and calculates transaction amounts per asset for specific years.

    Args:
        address (str): The blockchain address to analyze.
        anni_da_dichiarare (list): List of years to declare holdings.
    
    Returns:
        dict: Aggregated yearly holdings per asset.
    """
    # Retrieve and clean transaction data
    df_lista_movimenti = get_clean_dataframe(address)

    colonna_data = "data"             # Column for transaction dates
    colonna_currency = "currency"     # Column for currency symbol
    colonna_amount = "amount"         # Column for transaction amount
    colonna_in_or_out = 'in_or_out'   # Column indicating transaction direction (IN/OUT)
    
    # Convert the date column to datetime format for filtering
    df_lista_movimenti[colonna_data] = pd.to_datetime(df_lista_movimenti[colonna_data])
    totale_monete = df_lista_movimenti[colonna_currency].unique()
    
    # Initialize dictionaries to store accumulated amounts
    dict_amount = {"amount": 0, "amount_eur": 0}
    dict_year = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}
    dict_rep_fin_per_moneta = {moneta: copy.deepcopy(dict_year) for moneta in totale_monete}
    
    # Calculate yearly amounts for each asset
    for anno in anni_da_dichiarare:
        df_mov_per_anno = df_lista_movimenti[df_lista_movimenti[colonna_data].dt.year == anno]
        
        if not df_mov_per_anno.empty:
            for moneta in totale_monete:
                df_mov_per_anno_moneta = df_mov_per_anno[df_mov_per_anno[colonna_currency] == moneta]
                df_mov_per_anno_moneta_in = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_in_or_out] == 'IN']
                df_mov_per_anno_moneta_out = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_in_or_out] == 'OUT']
                
                # Accumulate amounts for incoming transactions
                if not df_mov_per_anno_moneta_in.empty:
                    dict_rep_fin_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_in[colonna_amount].sum()
                
                # Deduct amounts for outgoing transactions
                if not df_mov_per_anno_moneta_out.empty:
                    dict_rep_fin_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_out[colonna_amount].sum()

    # Finalize cumulative holdings and prices per year
    dict_rep_fin_per_moneta = cumulate_amount_and_price(anni_da_dichiarare, dict_rep_fin_per_moneta, totale_monete, calculate_price=False)
    return dict_rep_fin_per_moneta

if __name__ == "__main__":
    address = '...'  # Insert your blockchain address here

    # Specify years to declare holdings
    anni_da_dichiarare = [2020, 2021, 2022, 2023, 2024]

    # Calculate holdings per asset for each year
    dict_rep_fin_per_moneta = composizione_cosmos(address, anni_da_dichiarare)
    pprint(dict_rep_fin_per_moneta)
