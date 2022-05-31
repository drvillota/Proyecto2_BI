from utils.file_util import cargar_datos

# ubicacion insertion
def insert_query_ubicacion():
    
    insert = f"INSERT INTO ubicacion (Codigo_Ubicacion, Codigo_Departamento, Departamento, Codigo_Entidad, Entidad) VALUES "
    insertQuery = ""
    nombreArchivos = {"Demografía Y Poblacion", "Vivienda y Servicios Publicos", "Educacion", "Salud"}
    
    for archivo in nombreArchivos:
        dataframe = cargar_datos(archivo)

        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(DEFAULT,{row.Código_Departamento},\'{row.Departamento}\',{row.Código_Entidad},\'{row.Entidad}\');\n"
    
    return insertQuery

# periodo insertion
def insert_query_periodo():
    
    insert = f"INSERT INTO periodo (Codigo_Periodo, Año, Mes) VALUES "
    insertQuery = ""
    años = list(range(1984, 2023))
    
    for año in años:
        meses = list(range(1, 12))

        for mes in meses:
            insertQuery += insert + f"(\'{año}/{mes}\',{año},{mes});\n"
    
    return insertQuery

# demografia_poblacion insertion
def insert_query_demografia_poblacion(**kwargs):
    
    insert = f"INSERT INTO demografia_poblacion (Dimension_Demografia_Poblacion, Codigo_Ubicacion, Codigo_Periodo, Subcategoria_Demografia_Poblacion, Indicador, Dato_Numerico, Dato_Cualitativo, Fuente_Demografia_Poblacion, Unidad_Medida) VALUES"
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    
    for index, row in dataframe.iterrows():
        codigoUbicacion = f"(SELECT Codigo_Ubicacion FROM ubicacion WHERE Codigo_Departamento=\'{row.Código_Departamento}\' AND Codigo_Entidad=\'{row.Código_Entidad}\')"
        codigoPeriodo = f"(SELECT Codigo_Periodo FROM perioso WHERE Año=\'{row.Año}\' AND Mes=\'{row.Mes}\')"
        insertQuery += insert + f"SELECT \'{row.Dimensión}\', {codigoUbicacion}, {codigoPeriodo}, \'{row.Subcategoría}\', \'{row.Indicador}\', {row.Dato_Numérico}, \'{row.Dato_Cualitativo}\', \'{row.Fuente}\', \'{row.Unidad_De_Medida}\');\n"
    return insertQuery

# vivienda_servicios insertion
def insert_query_vivienda_servicios(**kwargs):
    
    insert = f"INSERT INTO vivienda_servicios (Dimension_Vivienda_Servicios, Codigo_Ubicacion, Codigo_Periodo, Subcategoria_Vivienda_Servicios, Indicador, Dato_Numerico, Dato_Cualitativo, Fuente_Vivienda_Servicios, Unidad_Medida) VALUES"
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    
    for index, row in dataframe.iterrows():
        codigoUbicacion = f"(SELECT Codigo_Ubicacion FROM ubicacion WHERE Codigo_Departamento=\'{row.Código_Departamento}\' AND Codigo_Entidad=\'{row.Código_Entidad}\')"
        codigoPeriodo = f"(SELECT Codigo_Periodo FROM perioso WHERE Año=\'{row.Año}\' AND Mes=\'{row.Mes}\')"
        insertQuery += insert + f"SELECT \'{row.Dimensión}\', {codigoUbicacion}, {codigoPeriodo}, \'{row.Subcategoría}\', \'{row.Indicador}\', {row.Dato_Numérico}, \'{row.Dato_Cualitativo}\', \'{row.Fuente}\', \'{row.Unidad_De_Medida}\');\n"
    return insertQuery

# educacion insertion
def insert_query_educacion(**kwargs):
    
    insert = f"INSERT INTO educacion (Dimension_Educacion, Codigo_Ubicacion, Codigo_Periodo, Subcategoria_Educacion, Indicador, Dato_Numerico, Dato_Cualitativo, Fuente_Educacion, Unidad_Medida) VALUES"
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    
    for index, row in dataframe.iterrows():
        codigoUbicacion = f"(SELECT Codigo_Ubicacion FROM ubicacion WHERE Codigo_Departamento=\'{row.Código_Departamento}\' AND Codigo_Entidad=\'{row.Código_Entidad}\')"
        codigoPeriodo = f"(SELECT Codigo_Periodo FROM perioso WHERE Año=\'{row.Año}\' AND Mes=\'{row.Mes}\')"
        insertQuery += insert + f"SELECT \'{row.Dimensión}\', {codigoUbicacion}, {codigoPeriodo}, \'{row.Subcategoría}\', \'{row.Indicador}\', {row.Dato_Numérico}, \'{row.Dato_Cualitativo}\', \'{row.Fuente}\', \'{row.Unidad_De_Medida}\');\n"
    return insertQuery

# salud insertion
def insert_query_salud(**kwargs):
    
    insert = f"INSERT INTO salud (Dimension_Salud, Codigo_Ubicacion, Codigo_Periodo, Subcategoria_Salud, Indicador, Dato_Numerico, Dato_Cualitativo, Fuente_Salud, Unidad_Medida) VALUES"
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    
    for index, row in dataframe.iterrows():
        codigoUbicacion = f"(SELECT Codigo_Ubicacion FROM ubicacion WHERE Codigo_Departamento=\'{row.Código_Departamento}\' AND Codigo_Entidad=\'{row.Código_Entidad}\')"
        codigoPeriodo = f"(SELECT Codigo_Periodo FROM perioso WHERE Año=\'{row.Año}\' AND Mes=\'{row.Mes}\')"
        insertQuery += insert + f"SELECT \'{row.Dimensión}\', {codigoUbicacion}, {codigoPeriodo}, \'{row.Subcategoría}\', \'{row.Indicador}\', {row.Dato_Numérico}, \'{row.Dato_Cualitativo}\', \'{row.Fuente}\', \'{row.Unidad_De_Medida}\');\n"
    return insertQuery
  
# fact_table insert
def insert_query_fact_table():
    
    insert = f"INSERT INTO fact_table (Fact_Key, Codigo_Ubicacion, Codigo_Periodo, Dimension_Demografia_Poblacion, Dimension_Vivienda_Servicios, Dimension_Educacion, Dimension_Salud) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"(SERIAL,\'{row.City_Key}\',\'{row.Customer_Key}\',\'{row.Stock_Item_Key}\',\'{row.Order_Date_Key}\',\'{row.Picked_Date_Key}\',\'{row.Salesperson_Key}\',\'{row.Picker_Key}\',\'{row.Package}\',\'{row.Quantity}\',\'{row.Unit_Price}\',\'{row.Tax_Rate}\',\'{row.Total_Excluding_Tax}\',\'{row.Tax_Amount}\',{row.Total_Including_Tax});\n"
    return insertQuery
