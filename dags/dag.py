from airflow.models.dag import dag
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import duckdb
import json
import requests
from datetime import datetime


# variables d'environnements

COLONNES_PRIM_DATA = [
    "station_id",
    "stationCode",
    "name",
    "lat",
    "lon",
    "capacity",
    "rental_methods"
]

# URL de l'endpoint PRIM
URL = "https://prim.iledefrance-mobilites.fr/marketplace/velib/station_information.json"
HEADERS = {
    "apiKey": "dSFU7sEc0sf2Ju3JZT0kqLa3qPnacJJB"
}
DATA_FILE_NAME = 'dags/data/stations.json'

@task
def get_data_from_prim_api(colonnes, url, headers, data_file_name):
    response = requests.get(url, headers=headers)
    response.raise_for_status() # Lève une exception si la requête échoue

    #Extraction uniquement des données stations
    data = response.json()
    stations = data.get("data", {}).get("stations", [])


# Sauvegarder les données extraites dans un fichier JSON
    with open(data_file_name, 'w') as f:
        json.dump(stations, f)

    #return data_file_name

@task
def check_row_numbers():
    conn = None
    nb_lignes = 0
    try:
        conn = duckdb.connect('dags/data/my_bdd_airflow.duckdb', read_only=True)
        nb_lignes = conn.sql(f"SELECT count(*) FROM my_bdd_airflow.main.data_from_prim").fetchone()[0]
    finally:
        if conn:
            conn.close()
    print(f"le nombre de lignes = {nb_lignes}")

@task
def check_duplicates():
    conn = None
    try:
        conn = duckdb.connect('dags/data/my_bdd_airflow.duckdb', read_only=True)
        nb_duplicates = conn.sql("""
                                 SELECT station_id, stationCode, lat, lon, count(*) AS ct
                                 FROM my_bdd_airflow.main.data_from_prim
                                 GROUP BY 1,2,3,4
                                 HAVING ct > 1;                                
                                 """).fetchall()
    finally:
        if conn:
            conn.close()
    print(f"Nombre de lignes dupliquées = {len(nb_duplicates)}")

@task
def load_from_file(data_file_name):
    conn = None
    try:
        conn = duckdb.connect('dags/data/my_bdd_airflow.duckdb')
        # Charger les données JSON dans la table duckDB
        conn.sql(f"INSERT INTO my_bdd_airflow.main.data_from_prim (SELECT * FROM '{data_file_name}')")
    finally:
        if conn:
            conn.close()

@dag(
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 25),
    catchup=False
)
def prim_pipelines():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Définissez la chaîne des tâches
    get_data_task = get_data_from_prim_api(COLONNES_PRIM_DATA, URL, HEADERS, DATA_FILE_NAME)
    load_data_task = load_from_file(DATA_FILE_NAME)
    check_row_task = check_duplicates()
    check_duplicates_task = check_row_numbers()

    start >> get_data_task >> load_data_task >> [check_row_task, check_duplicates_task] >> end

# Initialisation du DAG
prim_pipeline_dag = prim_pipelines()