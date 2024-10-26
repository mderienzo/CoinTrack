import pandas as pd
import numpy as np
import os




if __name__ == "__main__":
    path_to_xslx = "..."


    conio_xls = [os.path.join(path_to_xslx,file) for file in os.listdir(path_to_xslx) if file.endswith(".xlsx")]
    df_lista_movimenti = pd.read_excel(conio_xls[0])


    costo_servizio = df_lista_movimenti.loc[df_lista_movimenti['Acquisto/Vendita'].str.contains("Acquisto", case=False, na=False)]['Costo servizio'].sum()
    crypto_acquisto = df_lista_movimenti.loc[df_lista_movimenti['Acquisto/Vendita'].str.contains("Acquisto", case=False, na=False)]['Controvalore euro'].sum()

