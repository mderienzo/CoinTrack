import pandas as pd
import time
from datetime import datetime, timedelta
import yfinance as yahoo_finance
import os
from pprint import pprint
import copy
from utilities.functions import concat_dataframes, get_exchange_from_to_currency, get_coin_price_binance, cumulate_amount_and_price
from metadata.binance_metadata import add_amount

def calculate_coin_price(row):
    """
    Retrieves the price of a cryptocurrency in USDT at a specific timestamp.

    Args:
        row (pd.Series): A row of data containing the cryptocurrency symbol and timestamp.
    
    Returns:
        float: The price of the cryptocurrency at the specified timestamp.
    """
    coin_symbol = row['Coin'] + 'USDT'
    timestamp_str = row['UTC_Time']
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    coin_price = get_coin_price_binance(coin_symbol, timestamp)
    return coin_price

def remove_prefix(row):
    """
    Removes the prefix from the cryptocurrency symbol if the operation type is 'Savings Distribution'.

    Args:
        row (pd.Series): A row of data containing the cryptocurrency operation type and symbol.
    
    Returns:
        str: The modified or original cryptocurrency symbol.
    """
    if row['Operation'] == 'Savings Distribution':
        return row['Coin'][2:]
    else:
        return row['Coin']

def binance_composition(binance_csv, anni_da_dichiarare):
    """
    Processes and aggregates cryptocurrency transaction data per year and per asset.

    Args:
        binance_csv (list): List of paths to CSV files with transaction data.
        anni_da_dichiarare (list): List of years for which to declare holdings.

    Returns:
        tuple: 
            - dict_giacenze_per_moneta (dict): Yearly holdings per asset.
            - dict_giacenze_per_anno (dict): Yearly holdings overview for all assets.
    """
    
    # Concatenate all Binance CSV files into a single DataFrame
    df_lista_movimenti = concat_dataframes(*binance_csv)
    colonna_data = "UTC_Time"        # Date column in the transaction data
    colonna_currency = "Coin"        # Currency column (e.g., 'BTC')
    colonna_amount = "Change"        # Amount of asset change (positive or negative)
    colonna_tipo_azione = "Operation" # Operation type (e.g., 'Deposit', 'Savings Distribution')
    
    # Convert the date column to datetime format for filtering and processing
    df_lista_movimenti[colonna_data] = pd.to_datetime(df_lista_movimenti[colonna_data])
    totale_monete = df_lista_movimenti[colonna_currency].unique()   # List of unique assets
    totale_azioni = df_lista_movimenti[colonna_tipo_azione].unique() # List of unique operation types
    campi_da_calcolare = ["amount", "amount_eur"] # Fields to calculate for each asset

    # Initialize dictionaries to store data
    dict_amount = {campo: 0 for campo in campi_da_calcolare}
    dict_year = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}
    dict_giacenze_per_moneta = {moneta: copy.deepcopy(dict_year) for moneta in totale_monete}

    # Calculate yearly amounts for each asset
    for anno in anni_da_dichiarare:
        df_mov_per_anno = df_lista_movimenti[df_lista_movimenti[colonna_data].dt.year == anno]
        
        if not df_mov_per_anno.empty:
            for moneta in totale_monete:
                df_mov_per_anno_moneta = df_mov_per_anno[df_mov_per_anno[colonna_currency] == moneta]
                
                for azione in totale_azioni:
                    df_mov_per_anno_moneta_azione = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_tipo_azione] == azione]
                    
                    if not df_mov_per_anno_moneta_azione.empty:
                        # If action type is in add_amount metadata, accumulate amount changes for the asset
                        if azione in add_amount:
                            dict_giacenze_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione[colonna_amount].sum()
    
    # Finalize cumulative holdings and prices per year
    cumulate_amount_and_price(anni_da_dichiarare, dict_giacenze_per_moneta, totale_monete, calculate_price=False)

    # Build annual holdings view
    dict_per_moneta = {moneta: 0 for moneta in totale_monete}
    dict_amount = {campo: copy.deepcopy(dict_per_moneta) for campo in campi_da_calcolare}
    dict_giacenze_per_anno = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}

    # Assign calculated holdings per year to the summary dictionary
    for anno in anni_da_dichiarare:
        for moneta in totale_monete:
            dict_giacenze_per_anno[anno]["amount"][moneta] = dict_giacenze_per_moneta[moneta][anno]["amount"]
            dict_giacenze_per_anno[anno]["amount_eur"][moneta] = dict_giacenze_per_moneta[moneta][anno]["amount_eur"]
    
    return dict_giacenze_per_moneta, dict_giacenze_per_anno

if __name__ == "__main__":
    # Specify the path to the CSV directory
    path_to_csv = "..."

    # Create a list of all CSV file paths in the directory
    binance_csv = [os.path.join(path_to_csv, file) for file in os.listdir(path_to_csv) if file.endswith(".csv")]

    # Calculate holdings for specified years
    dict_giacenze_per_moneta, dict_giacenze_per_anno = binance_composition(
        binance_csv=binance_csv,
        anni_da_dichiarare=[2020, 2021, 2022, 2023, 2024]
    )

    # Print the annual holdings summary
    pprint(dict_giacenze_per_anno)
