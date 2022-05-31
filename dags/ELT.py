# Utilidades de airflow
from airflow.models import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

# Utilidades de python
from datetime  import datetime

# Funciones ETL
from utils.crear_tablas import crear_tablas
from utils.insert_queries import *
from utils.obtener_datos import *

default_args= {
    'owner': 'Estudiante',
    'email_on_failure': False,
    'email': ['estudiante@uniandes.edu.co'],
    'start_date': datetime(2022, 5, 5) # inicio de ejecución
}

with DAG(
    "ETL",
    description='ETL',
    schedule_interval='@daily', # ejecución diaría del DAG
    default_args=default_args, 
    catchup=False) as dag:

    #procesamiento_datos = PythonOperator(
    #task_id='procesar_datos_csv',
    #provide_context=True,
    #python_callable=obtener_datos
    #)
    obtener_datos()

    # task: 1 crear las tablas en la base de datos postgres
    crear_tablas_db = PostgresOperator(
    task_id="crear_tablas_en_postgres",
    postgres_conn_id="postgres_localhost", # Nótese que es el mismo ID definido en la conexión Postgres de la interfaz de Airflow
    sql= crear_tablas()
    )

    # task: 2 poblar las tablas de dimensiones en la base de datos
    with TaskGroup('poblar_tablas') as poblar_tablas_dimensiones:

        # task: 2.1 poblar tabla ubicacion
        poblar_ubicacion = PostgresOperator(
            task_id="poblar_ubicacion",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_ubicacion()
        )

        # task: 2.2 poblar tabla periodo
        poblar_periodo = PostgresOperator(
            task_id="poblar_periodo",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_periodo()
        )

        # task: 2.3 poblar tabla demografia_poblacion
        poblar_demografia_poblacion = PostgresOperator(
            task_id="poblar_demografia_poblacion",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_demografia_poblacion(csv_path ="Demografía Y Poblacion clean")
        )

        # task: 2.4 poblar tabla vivienda_servicios
        poblar_vivienda_servicios = PostgresOperator(
            task_id="poblar_vivienda_servicios",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_vivienda_servicios(csv_path = "Vivienda y Servicios Publicos clean")
        )

        # task: 2.5 poblar tabla educacion
        poblar_educacion = PostgresOperator(
            task_id="poblar_educacion",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_educacion(csv_path = "Educacion clean")
        )

        # task: 2.6 poblar tabla salud
        poblar_salud = PostgresOperator(
            task_id="poblar_salud",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_salud(csv_path = "Salud clean")
        )

    
    # flujo de ejecución de las tareas  
    crear_tablas_db >> poblar_tablas_dimensiones
