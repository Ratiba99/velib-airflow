from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import time
from pymongo import MongoClient

# URL de l'API Vélib Open Data
url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel"

def fetch_velib_data():
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['records']  # Accède aux enregistrements spécifiques
    else:
        print("Erreur lors de la récupération des données")
        return None

def transform_data(data):
    df = pd.DataFrame([record['fields'] for record in data])
    print("Données transformées :")
    print(df.head())
    return df

def clean_data(df):
    # Suppression des valeurs manquantes
    df.dropna(inplace=True)

    # Conversion des formats (dates, types numériques)
    df['duedate'] = pd.to_datetime(df['duedate'])
    df['numbikesavailable'] = df['numbikesavailable'].astype(int)
    df['numdocksavailable'] = df['numdocksavailable'].astype(int)

    print("Données nettoyées :")
    print(df.head())
    return df

def calculate_indicators(df):
    # Taux de remplissage par station
    df['fill_rate'] = df['numbikesavailable'] / df['capacity']

    # Moyenne de vélos disponibles par arrondissement
    avg_bikes_per_arrondissement = df.groupby('nom_arrondissement_communes')['numbikesavailable'].mean()

    print("Indicateurs calculés :")
    print(df.head())
    print("Moyenne de vélos disponibles par arrondissement :")
    print(avg_bikes_per_arrondissement)

    return df, avg_bikes_per_arrondissement

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 2, 7),  # A adapter selon la date de début que tu souhaites
}

dag = DAG(
    'velib_dag',  # Nom de ton DAG
    default_args=default_args,
    description='DAG pour récupérer et traiter les données Vélib',
    schedule_interval='* * * * *',  # Exécution toutes les minutes
    catchup=False,
)

# Définir les opérateurs Python pour exécuter chaque fonction
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_velib_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[fetch_task.output],  # Utilise la sortie de la tâche précédente
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    op_args=[transform_task.output],  # Utilise la sortie de la tâche précédente
    dag=dag,
)

calculate_task = PythonOperator(
    task_id='calculate_indicators',
    python_callable=calculate_indicators,
    op_args=[clean_task.output],  # Utilise la sortie de la tâche précédente
    dag=dag,
)

# Définir la séquence d'exécution des tâches
fetch_task >> transform_task >> clean_task >> calculate_task
