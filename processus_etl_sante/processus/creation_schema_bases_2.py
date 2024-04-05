import psycopg2 as ps
from processus_etl_sante.processus import db_parametres_0

connecte= db_parametres_0.connexion()

def psg_parametres_raw():
    
    query='''create schema if not exists stage;

            create table if not exists stage.chirurgies(
                Autres varchar,
                Chirurgie_generale varchar,
                Chirurgie_orthopedique varchar,
                Chirurgie_plastique varchar,
                Chirurgie_vasculaire varchar,
                Delais_dattente varchar,
                Neurochirurgie varchar,
                ORL_chirurgie_cervico_faciale varchar,
                Obstetrique_et_gynecologie varchar,
                Ophtalmologie varchar,
                PeriodeAttente varchar,
                Region varchar,
                Total varchar,
                Urologie varchar
            );

            create table if not exists stage.regions(
                Reference varchar,
                Region varchar
            )'''
    connecte.execute(query)

def psg_parametres_mart():

    query='''create schema if not exists mart; 

             create table if not exists mart.chirurgies(
                periode_attente varchar,
                region varchar,
                chirurgie_generale bigint,
                chirurgie_orthopedique bigint,
                chirurgie_plastique bigint,
                chirurgie_vasculaire bigint,
                neurochirurgie bigint,
                obstetrique_et_gynecologie bigint,
                ophtalmologie bigint,
                orl_chirurgie_cervico_faciale bigint,
                urologie bigint,
                autres bigint,
                delais_dattente varchar,
                total varchar
             );

             create table if not exists mart.chirurgies_raffine(
                periode_attente varchar,
                annee_financiere_1 varchar,
                annee_financiere_2 varchar,
                periode varchar,
                region varchar,
                chirurgie_generale bigint,
                chirurgie_orthopedique bigint,
                chirurgie_plastique bigint,
                chirurgie_vasculaire bigint,
                neurochirurgie bigint,
                obstetrique_et_gynecologie bigint,
                ophtalmologie bigint,
                orl_chirurgie_cervico_faciale bigint,
                urologie bigint,
                autres bigint,
                delais_dattente varchar,
                total bigint
            );
             
             create table if not exists mart.regions(
                 Reference varchar primary key not null,
                 Region varchar
            )'''
    connecte.execute(query)

