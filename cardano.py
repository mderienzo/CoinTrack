import pandas as pd
from pprint import pprint
import os
import copy
from utilities.functions import get_coin_price_yahoo, concat_dataframes, cumulate_amount_and_price
from datetime import timedelta

def get_coin_value(row):
    """
    Retrieves the value of a cryptocurrency at a specific date from Yahoo Finance.

    Args:
        row (pd.Series): A row containing the date for the coin price lookup.
    
    Returns:
        float or None: The value of the cryptocurrency at the specified date or None if an error occurs.
    """
    try:
        coin = "ADA-EUR"  # Fixed symbol for ADA/EUR pair
        value = get_coin_price_yahoo(coin, row["Date"], "1d")
        return value
    except:
        return None

def composizione_cardano(cardano_csv, anni_da_dichiarare):
    """
    Processes and aggregates Cardano transaction data per year and per asset for reporting.

    Args:
        cardano_csv (list): List of paths to CSV files with transaction data.
        anni_da_dichiarare (list): List of years for which to declare holdings.

    Returns:
        dict: Yearly holdings per asset.
    """
    
    # Read and concatenate all Yoroi Ledger CSV files
    df_lista_movimenti = concat_dataframes(*cardano_csv)
    colonna_data = "Date"                       # Date column in transaction data
    colonna_currency_buy = "Buy Cur."           # Column for the currency being bought
    colonna_currency_sell = "Sell Cur."         # Column for the currency being sold
    colonna_amount_buy = "Buy Amount"           # Amount of asset being bought
    colonna_amount_sell = "Sell Amount"         # Amount of asset being sold
    colonna_tipo_azione = "Type (Trade, IN or OUT)" # Operation type (e.g., 'Trade', 'IN', 'OUT')
    colonna_fee = 'Fee Amount (optional)'       # Column for transaction fees

    # Convert date column to datetime format for filtering and processing
    df_lista_movimenti[colonna_data] = pd.to_datetime(df_lista_movimenti[colonna_data])

    # Fill NaN values in certain columns to prevent issues in calculations
    df_lista_movimenti.fillna({colonna_amount_buy: 0}, inplace=True)
    df_lista_movimenti.fillna({colonna_amount_sell: 0}, inplace=True)
    df_lista_movimenti.fillna({colonna_fee: 0}, inplace=True)

    # Extract unique coins that appear in both Buy and Sell columns
    totale_monete = list(set(df_lista_movimenti[colonna_currency_buy].unique()) & set(df_lista_movimenti[colonna_currency_sell].unique()))
    totale_monete = [moneta for moneta in totale_monete if isinstance(moneta, str)]
    totale_azioni = df_lista_movimenti[colonna_tipo_azione].unique()  # List of unique transaction types

    # Initialize dictionaries to store accumulated amounts
    dict_amount = {"amount": 0, "amount_eur": 0}
    dict_year = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}
    dict_rep_fin_per_moneta = {moneta: copy.deepcopy(dict_year) for moneta in totale_monete}

    # Calculate yearly amounts for each asset
    for anno in anni_da_dichiarare:
        df_mov_per_anno = df_lista_movimenti[df_lista_movimenti[colonna_data].dt.year == anno]
        
        if not df_mov_per_anno.empty:
            for moneta in totale_monete:
                df_mov_per_anno_moneta = df_mov_per_anno.loc[(df_mov_per_anno[colonna_currency_buy] == moneta) | (df_mov_per_anno[colonna_currency_sell] == moneta)]
                
                for azione in totale_azioni:
                    df_mov_per_anno_moneta_azione = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_tipo_azione] == azione]
                    
                    if not df_mov_per_anno_moneta_azione.empty:
                        # Accumulate amounts for "Deposit" transactions and subtract fees
                        if azione in ["Deposit"]:
                            dict_rep_fin_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione[colonna_amount_buy].sum()
                            dict_rep_fin_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_azione[colonna_fee].sum()
                        
                        # Deduct amounts for "Withdrawal" transactions and subtract fees
                        if azione in ["Withdrawal"]:
                            dict_rep_fin_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_azione[colonna_amount_sell].sum()
                            dict_rep_fin_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_azione[colonna_fee].sum()

    # Finalize cumulative holdings and prices per year
    cumulate_amount_and_price(anni_da_dichiarare, dict_rep_fin_per_moneta, totale_monete, calculate_price=False)

    return dict_rep_fin_per_moneta

if __name__ == "__main__":
    # Specify the path to the directory containing CSV files
    path_to_csv = "..."
    
    # Create a list of all CSV file paths in the directory
    cardano_csv = [os.path.join(path_to_csv, file) for file in os.listdir(path_to_csv) if file.endswith(".csv")]

    # Calculate holdings for specified years
    dict_rep_fin_per_moneta = composizione_cardano(cardano_csv, anni_da_dichiarare=[2020, 2021, 2022, 2023, 2024])

    # Display the aggregated results
    pprint(dict_rep_fin_per_moneta)
