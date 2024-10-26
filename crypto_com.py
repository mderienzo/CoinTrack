import pandas as pd
import os
import copy
from metadata.crypto_com_metadata import add_amount, sub_amount, return_withdraw_transactions, return_deposit_transactions, return_buy_transactions, return_exchange_transactions, return_reward_transactions, return_recurring_buy_transactions, return_transfer_transactions
from pprint import pprint
from utilities.functions import concat_dataframes, cumulate_amount_and_price
from datetime import timedelta




def composizione_crypto_com(crypto_com_csv, anni_da_dichiarare):


    df_lista_movimenti = concat_dataframes(*crypto_com_csv)
    colonna_data = 'Timestamp (UTC)'
    colonna_currency = "Currency"
    colonna_to_currency = "To Currency"
    colonna_amount = "Amount"
    colonna_to_amount = "To Amount"
    colonna_tipo_azione = "Transaction Description"
    # Converti la colonna in datetime
    df_lista_movimenti[colonna_data] = pd.to_datetime(df_lista_movimenti[colonna_data])
    to_currency = set((df_lista_movimenti[~df_lista_movimenti[colonna_to_currency].isna()][colonna_to_currency].unique()))
    currency = set(df_lista_movimenti[colonna_currency].unique())
    totale_monete = list(currency | to_currency)
    totale_azioni = df_lista_movimenti[colonna_tipo_azione].unique()
    campi_da_calcolare = ["amount", "amount_eur"]
    dict_amount = {campo: 0 for campo in campi_da_calcolare}
    dict_year = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}
    dict_giacenze_per_moneta = {moneta: copy.deepcopy(dict_year) for moneta in totale_monete}

    withdraw_transactions = return_withdraw_transactions(df_lista_movimenti)
    deposit_transactions = return_deposit_transactions(df_lista_movimenti)
    buy_transactions = return_buy_transactions(df_lista_movimenti)
    exchange_transactions = return_exchange_transactions(df_lista_movimenti)
    reward_transactions = return_reward_transactions(df_lista_movimenti)
    recurring_buy_transactions = return_recurring_buy_transactions(df_lista_movimenti)
    transfer_transactions = return_transfer_transactions(df_lista_movimenti)
    for anno in anni_da_dichiarare:
        df_mov_per_anno = df_lista_movimenti[df_lista_movimenti[colonna_data].dt.year == anno]
        if not df_mov_per_anno.empty:
            for moneta in totale_monete:
                df_mov_per_anno_moneta = df_mov_per_anno[df_mov_per_anno[colonna_currency] == moneta]
                for azione in totale_azioni:
                    df_mov_per_anno_moneta_azione = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_tipo_azione] == azione]
                    if not df_mov_per_anno_moneta_azione.empty:
                        if azione in add_amount or azione in deposit_transactions or azione in buy_transactions or azione in reward_transactions or azione in transfer_transactions:
                            dict_giacenze_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione[colonna_amount].sum()
                        if azione in recurring_buy_transactions:
                            dict_giacenze_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione[colonna_to_amount].sum()
                            # dict_giacenze_per_moneta[moneta][anno]["amount_eur"] += df_mov_per_anno_moneta_azione["Native Amount"].sum()
                        if azione in withdraw_transactions or azione in sub_amount:
                            dict_giacenze_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione[colonna_amount].sum()
                            # dict_giacenze_per_moneta[moneta][anno]["amount_eur"] -= df_mov_per_anno_moneta_azione["Native Amount"].sum()
                        if azione in exchange_transactions:
                            dict_giacenze_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione[colonna_amount].sum()
                            # dict_giacenze_per_moneta[moneta][anno]["amount_eur"] -= df_mov_per_anno_moneta_azione["Native Amount"].sum()
                            lista_monete_ottenute = df_mov_per_anno_moneta_azione[colonna_to_currency].unique()
                            for moneta_ottenuta in lista_monete_ottenute:
                                dict_giacenze_per_moneta[moneta_ottenuta][anno]["amount"] += df_mov_per_anno_moneta_azione[df_mov_per_anno_moneta_azione[colonna_to_currency] == moneta_ottenuta][colonna_to_amount].sum()
    cumulate_amount_and_price(anni_da_dichiarare, dict_giacenze_per_moneta, totale_monete, calculate_price = False)

    # COSTRUZIONE DELLA VISTA DELLE GIACENZE PER ANNO
    dict_per_moneta = {moneta: 0 for moneta in totale_monete}
    dict_amount = {campo: copy.deepcopy(dict_per_moneta) for campo in campi_da_calcolare}
    dict_giacenze_per_anno = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}

    for anno in anni_da_dichiarare:
        for moneta in totale_monete:
            dict_giacenze_per_anno[anno]["amount"][moneta] = dict_giacenze_per_moneta[moneta][anno]["amount"]
            dict_giacenze_per_anno[anno]["amount_eur"][moneta] = dict_giacenze_per_moneta[moneta][anno]["amount_eur"]


    return dict_giacenze_per_moneta, dict_giacenze_per_anno


if __name__ == "__main__":
    
    path_to_csv = "..."

    crypto_com_csv = [os.path.join(path_to_csv, file) for file in os.listdir(path_to_csv) if file.startswith("transazioni_crypto_wallet")]
    dict_giacenze_per_moneta, dict_giacenze_per_anno = composizione_crypto_com(crypto_com_csv=crypto_com_csv, anni_da_dichiarare=[2020, 2021, 2022, 2023, 2024])

    pprint(dict_giacenze_per_anno)
    pprint(dict_giacenze_per_moneta)
