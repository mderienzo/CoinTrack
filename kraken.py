import krakenex
from datetime import datetime, timedelta
import time
import pandas as pd
import os
import copy
from pprint import pprint
from utilities.functions import get_coin_price_kraken, concat_dataframes, cumulate_amount_and_price

from metadata.kraken_metadata import mapping_coin




def composizione_kraken(kraken_csv, anni_da_dichiarare):
    df_lista_movimenti = concat_dataframes(*kraken_csv)

    df_lista_movimenti['asset'] = df_lista_movimenti['asset'].str.replace(r'[0-9\.].*$', '', regex = True)

    colonna_data = "time"
    colonna_currency = "asset"
    colonna_amount = "amount"
    colonna_tipo_azione = "type"
    colonna_fee = "fee"
    # Converti la colonna in datetime
    df_lista_movimenti[colonna_data] = pd.to_datetime(df_lista_movimenti[colonna_data])
    totale_monete = df_lista_movimenti[colonna_currency].unique()
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
                for azione in totale_azioni:
                    df_mov_per_anno_moneta_azione = df_mov_per_anno_moneta[df_mov_per_anno_moneta[colonna_tipo_azione] == azione]
                    if not df_mov_per_anno_moneta_azione.empty:
                        if azione in ["earn", 'deposit', 'transfer', 'staking', 'spend', 'receive', 'trade']:
                            dict_giacenze_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione[colonna_amount].sum()
                            dict_giacenze_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_azione[colonna_fee].sum()
    cumulate_amount_and_price(anni_da_dichiarare, dict_giacenze_per_moneta, totale_monete, finestra_temporale_yahoo=5, calculate_price=False)

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
    path_csv = "..."
    kraken_csv = [os.path.join(path_csv, file) for file in os.listdir(path_csv) if file.endswith(".csv")]
    dict_giacenze_per_moneta, dict_giacenze_per_anno = composizione_kraken(kraken_csv=kraken_csv, anni_da_dichiarare=[2020, 2021, 2022, 2023, 2024])

    pprint(dict_giacenze_per_anno)

