import psycopg2 as pg
from dotenv import load_dotenv 
import os

load_dotenv()

def connexion():
    psg_c = pg.connect(database=os.getenv("database"),
                    user=os.getenv("user"),
                    password=os.getenv("password"),
                    host=os.getenv("host"),
                    port=os.getenv("port")
                     )
    connecte = psg_c.cursor()

    psg_c.autocommit = True
    return connecte 