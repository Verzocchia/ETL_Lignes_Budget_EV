xml_to_parquet.py

    le schéma a encore des commentaires, qu'en est-il ?
    les fonctions recherche id à supprimer au profit d'un select en database lors du traitement
    _isolement_id c'est plutôt extraction_id, je trouve ton code compliqué, ça doit faire 2 lignes normalement
    travail_mtsup_ligne, travail_caracsup_ligne, il me semble que s'il y a plusieurs fois le même code, la donnée est écrasé par la dernière valeur trouvé
    xml_to_parquet, try except à mieux focaliser, ne pas le faire au global
    xml_to_parquet, séparer les infos qui sont de l'ordre du document budgétaire, du budget et des annexes. -> préparer l'arrivée de la base de données avec clés primaires, étrangères... ça aidera je pense Jamila à faire des jointures dans Superset.
    xml_to_parquet, docbase = lignes_budget, i = ligne

concat.ipynb

    Schema en pyarrow. Dans l'idéal, il faudra utiliser un ORM, pg2 ou du SQL. Pour l'instant nous avons travailler avec Parquet parce que c'était pratique, maintenant il faut que nous rendons notre code plus durable et l'adaptons à la stratégie Airflow.
    Exer en float ?
    sortir le comptage des doublons dans un autre fichier
    pour le comptage, il faut raisonner sur les documents budgétaires et pas sur les budgets
    il faut séparer ce qui est de l'explo de ce qui est du code qu'on mettra dans Airflow. 
    date_precise


- voir les numdec sur 20k 
- vérifier pandas.to_sql (passage de NaT en None)

12_02 : 

- Passer de psycog2 à sql alchemy 

- types dans les variables des fonctions (conn : connection.psycho2, etc.) -> pd.DataFrame : 
- créer un fichier sql_requetes.py (contient les tables) pour les importer 
- vérifier les scripts if __main__ = main 
- Laisser l'ID par caractère 

- Creer une fonction à deux cas : 
si on doit simplement créer des tables ou si on doit suppr et créer $


- Crea par bloc = 

faire simplement un .rename(col= ), pareil avc mtsup
'TypOpe',
    'Section',
    'ChapSpe',
    'ProgAutoLib',
    'ProgAutoNum',
    'VirCredNum',
    'CodeRegion'

- isolement id to extract_id 

extraction_id




{'Nature': {'@V': '66112'},
     'Fonction': {'@V': '90'},
     'ContNat': {'@V': '66'},
     'ArtSpe': {'@V': 'false'},
     'CodRD': {'@V': 'D'},
     'MtBudgPrec': {'@V': '-2070.00'},
     'MtPropNouv': {'@V': '-2171.00'},
     'MtPrev': {'@V': '-2171.00'},
     'CredOuv': {'@V': '-2171.00'},
     'OpBudg': {'@V': '0'},
     'MtSup': [{'@V': '-2070.00', '@Code': 'BudgetHorsRAR'},
      {'@V': '16275.95', '@Code': 'ICNE'},
      {'@V': '18446.95', '@Code': 'ICNEPrec'}]

___________________________________________________
{'Nature' : '66112',
'Fonction' : '90', 
....

'MtSup' : '[{'@V': '-2070.00', '@Code': 'BudgetHorsRAR'},
      {'@V': '16275.95', '@Code': 'ICNE'},
      {'@V': '18446.95', '@Code': 'ICNEPrec'}]',

'BudgetHorsRAR' : '-2070',
'ICNE' : '1642,6',
'ICNEPrec' : '18446.95'}


- M



------------------------------

- passage vers sql alchemy + revoir les schéma (les @ qui trainent peut être) 



methodes to_sql  many + 

# Alternative to_sql() *method* for DBs that support COPY FROM
import csv
from io import StringIO

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)