from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from processus_etl_sante.processus import donnees_api_1
from processus_etl_sante.processus import creation_schema_bases_2
from processus_etl_sante.processus import chargement_donnees_3


with DAG("ETL_sante",
         start_date=datetime(2024,1,30),
         schedule_interval='0 2 * * *',
         catchup=False) as dag:
    
    tache1 = PythonOperator(
        task_id='données_api_chirurgie',
        python_callable=donnees_api_1.api_chirurgie)

    tache2 = PythonOperator(
        task_id='données_api_region',
        python_callable=donnees_api_1.api_region)

    tache3 = PythonOperator(
        task_id='création_dépendances_raw',
        python_callable=creation_schema_bases_2.psg_parametres_raw)

    tache4 = PythonOperator(
        task_id='création_dépendances_mart',
        python_callable=creation_schema_bases_2.psg_parametres_mart)

    tache5 = PythonOperator(
        task_id='chargement_donnees_raw',
        python_callable=chargement_donnees_3.chargement_raw)
    
    tache6 = PythonOperator(
        task_id='chargement_donnees_mart',
        python_callable=chargement_donnees_3.chargement_mart)

    tache7 = PythonOperator(
        task_id='chargement_donnees_mart_rafinne',
        python_callable=chargement_donnees_3.chargement_mart_raffine)
        

[tache1,tache2,tache3,tache4]>>tache5>>tache6>>tache7
