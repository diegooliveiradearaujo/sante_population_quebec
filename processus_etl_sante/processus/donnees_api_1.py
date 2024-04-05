import pandas as pd
import json as js
import requests as r
from dotenv import load_dotenv 
import os

load_dotenv()
api_nosql_sante=os.getenv("api")

def api_chirurgie():
    reponse = r.get(f'{api_nosql_sante}/donnees_sante/-NuF2gv_rir3IDuIw45V.json')
    reponse = reponse.json()
    dataframe = pd.DataFrame(reponse)
    dataframe.to_csv("/home/diego/airflow/dags/processus_etl_sante/bases/raw_donnees_chirurgies.csv",index=False)

def api_region():
    reponse = r.get(f'{api_nosql_sante}/donnees_sante/-NuFBT47b89FK8PSizT5.json')
    reponse = reponse.json()
    dataframe = pd.DataFrame(reponse)
    dataframe.to_csv("/home/diego/airflow/dags/processus_etl_sante/bases/raw_donnees_regions.csv",index=False)




