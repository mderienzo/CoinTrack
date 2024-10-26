import pandas as pd
import numpy as np
import os
from pprint import pprint
import matplotlib.pyplot as plt
import copy

from utilities.functions import cumulate_amount_and_price


def composizione_swissborg(swissborg_xls, anni_da_dichiarare):

    df_lista_movimenti = pd.read_excel(swissborg_xls[1])
    # Converti la colonna in datetime
    df_lista_movimenti['Local time'] = pd.to_datetime(df_lista_movimenti['Local time'])
    totale_monete = df_lista_movimenti["Currency"].unique()
    totale_azioni = df_lista_movimenti["Type"].unique()

    dict_amount = {
        "amount": 0,
        "amount_eur": 0
    }
    dict_year = {anno: copy.deepcopy(dict_amount) for anno in anni_da_dichiarare}
    dict_rep_fin_per_moneta = {moneta: copy.deepcopy(dict_year) for moneta in totale_monete}
    for anno in anni_da_dichiarare:
        df_mov_per_anno = df_lista_movimenti[df_lista_movimenti['Local time'].dt.year == anno]
        if not df_mov_per_anno.empty:
            for moneta in totale_monete:
                df_mov_per_anno_moneta = df_mov_per_anno[df_mov_per_anno["Currency"] == moneta]
                for azione in totale_azioni:
                    df_mov_per_anno_moneta_azione = df_mov_per_anno_moneta[df_mov_per_anno_moneta["Type"] == azione]
                    if not df_mov_per_anno_moneta_azione.empty:
                        if azione in ["Deposit", "Buy", "Payouts"]:
                            dict_rep_fin_per_moneta[moneta][anno]["amount"] += df_mov_per_anno_moneta_azione["Net amount"].sum()
                        if azione in ["Sell", "Withdrawal"]:
                            dict_rep_fin_per_moneta[moneta][anno]["amount"] -= df_mov_per_anno_moneta_azione["Net amount"].sum()

    dict_rep_fin_per_moneta = cumulate_amount_and_price(anni_da_dichiarare, dict_rep_fin_per_moneta, totale_monete, calculate_price=False)

    return dict_rep_fin_per_moneta

if __name__ == "__main__":
    path_xlsx = "..."
    anni_da_dichiarare = [2020, 2021, 2022, 2023, 2024]
    swissborg_xls = [os.path.join(path_xlsx,file) for file in os.listdir(path_xlsx) if file.endswith(".xlsx")]
    dict_rep_fin_per_moneta = composizione_swissborg(swissborg_xls, anni_da_dichiarare)

    pprint(dict_rep_fin_per_moneta, width=60)
