from urllib import request
import pandas as pd
import os

def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/" + name + " clean.csv", sep=',', encoding = 'latin-1', index_col=False)
    return df

def guardar_datos(df, nombre):
    df.to_csv('/opt/airflow/data/' + nombre + '.csv' , encoding = 'latin-1', sep=',', index=False)
