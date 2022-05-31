from urllib import request
from utils.file_util import guardar_datos
import shutil
import os.path as path

# import pandas and numpy
import pandas as pd
import numpy as np
import sys

def obtener_datos():

    nombreArchivos = {"Demografía Y Poblacion", "Vivienda y Servicios Publicos", "Educacion", "Salud"}

    for nombre in nombreArchivos :

        if not path.exists('/opt/airflow/data/'+nombre+' Clean.csv'):
            #Leemos el archivo
            dataClean = pd.DataFrame(pd.read_excel('./data/'+nombre+'.xlsx'))
            dataClean = dataClean.drop(dataClean.index[0])

            #Verficamos los atributos
            dataClean['Código Departamento'] = pd.to_numeric(dataClean['Código Departamento'], errors='coerce')
            dataClean['Código Entidad'] = pd.to_numeric(dataClean['Código Entidad'], errors='coerce')
            dataClean['Dato Numérico'] = pd.to_numeric(dataClean['Dato Numérico'].str.replace('.','').str.replace(',', '.'), errors='coerce')
            dataClean['Dato Cualitativo'] = dataClean['Dato Cualitativo'].apply(str)
            dataClean['Año'] = pd.to_numeric(dataClean['Año'], errors='coerce')
            dataClean['Mes'] = pd.to_numeric(dataClean['Mes'], errors='coerce')

            #Renombramos las columnas
            dataClean.rename(columns = {'Código Departamento':'Código_Departamento', 'Código Entidad':'Código_Entidad', 'Dato Numérico':'Dato_Numérico', 'Dato Cualitativo':'Dato_Cualitativo', 'Unidad de Medida':'Unidad_de_Medida'}, inplace = True)

            #Borramos los datos que no aportan información
            dataClean = dataClean.dropna(subset=['Dato_Numérico'])

            guardar_datos(dataClean, nombre + " Clean")