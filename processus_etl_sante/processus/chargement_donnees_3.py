import psycopg2 as ps
from processus_etl_sante.processus import db_parametres_0

connecte= db_parametres_0.connexion()

def chargement_raw():
    nettoyage_base_1 = "truncate stage.chirurgies"

    connecte.execute(nettoyage_base_1)

    query_1= '''COPY stage.chirurgies(Autres,Chirurgie_generale,Chirurgie_orthopedique,Chirurgie_plastique,Chirurgie_vasculaire,Delais_dattente,Neurochirurgie,ORL_chirurgie_cervico_faciale,Obstetrique_et_gynecologie,Ophtalmologie,PeriodeAttente,Region,Total,Urologie)
    FROM '/home/diego/airflow/dags/processus_etl_sante/bases/raw_donnees_chirurgies.csv' DELIMITER ',' CSV HEADER;'''

    connecte.execute(query_1)

    nettoyage_base_2 = "truncate stage.regions"

    connecte.execute(nettoyage_base_2)

    query_2= '''COPY stage.regions(Reference,Region)
    FROM '/home/diego/airflow/dags/processus_etl_sante/bases/raw_donnees_regions.csv' DELIMITER ',' CSV HEADER;'''

    connecte.execute(query_2)

def chargement_mart():

    nettoyage_base_1 = "truncate mart.chirurgies"

    connecte.execute(nettoyage_base_1)
    
    query_1= '''insert into mart.chirurgies(periode_attente,region,chirurgie_generale,chirurgie_orthopedique,chirurgie_plastique,chirurgie_vasculaire,neurochirurgie,obstetrique_et_gynecologie,ophtalmologie,orl_chirurgie_cervico_faciale,urologie,autres,delais_dattente,total)
                select
                periodeattente,
                region,
                cast(chirurgie_generale as bigint),
                cast(chirurgie_orthopedique as bigint),
                cast(chirurgie_plastique as bigint),
                cast(chirurgie_vasculaire as bigint),
                cast(neurochirurgie as bigint),
                cast(obstetrique_et_gynecologie as bigint),
                cast(ophtalmologie as bigint),
                cast(orl_chirurgie_cervico_faciale as bigint),
                cast(urologie as bigint),
                cast(autres as bigint),
                delais_dattente,
                total
                from stage.chirurgies''' 


    connecte.execute(query_1)

    query_2= '''insert into mart.regions(reference,region)
                select reference, region from stage.regions
                on conflict (reference) do update
                SET region = excluded.region'''

    connecte.execute(query_2)

def chargement_mart_raffine():

    nettoyage_base_1 = "truncate mart.chirurgies_raffine"

    connecte.execute(nettoyage_base_1)

    query='''insert into mart.chirurgies_raffine(periode_attente,annee_financiere_1,annee_financiere_2,periode,region,chirurgie_generale,chirurgie_orthopedique,chirurgie_plastique,chirurgie_vasculaire,neurochirurgie,obstetrique_et_gynecologie,ophtalmologie,orl_chirurgie_cervico_faciale,urologie,autres,delais_dattente,total)
             select
             periode_attente, 
             '20'||SUBSTRING(split_part(periode_attente,'-',1), 1,2)  as annee_financiere_1,
             '20'||SUBSTRING(split_part(periode_attente,'-',1), 3,4)  as annee_financiere_2,
             split_part(periode_attente,'-',2) as periode,
             region,
             chirurgie_generale,chirurgie_orthopedique,
             chirurgie_plastique,chirurgie_vasculaire,
             neurochirurgie,obstetrique_et_gynecologie,
             ophtalmologie,orl_chirurgie_cervico_faciale,urologie,
             autres,
             REPLACE(delais_dattente,'�','-') as delais_dattente,
             cast(total as bigint)
             from mart.chirurgies
            '''
    connecte.execute(query)
