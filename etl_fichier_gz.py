import time 
import logging
import os
import sqlite3
import pathlib
import glob
import gzip 

import pandas as pd
import xmltodict

DOSSIER_PARENT = r" "
DOSSIER_SOURCE = r"./traitement en cours/"
DOSSIER_SORTIE = r"./traite/"
BDD = 'bdd_actes_budgetaires_gz.db'
NOM_CSV = 'donnees_budgetaires.csv'

logging.basicConfig(level=logging.INFO,
                    filename='traitement.log', 
                    filemode='w')

def timing_decorator(func):
 def wrapper(*args, **kwargs):
  start_time = time.time()
  result = func(*args, **kwargs)
  end_time = time.time()
  execution_time = end_time - start_time
  print(f"{func.__name__} a pris {execution_time:.4f} secondes pour s'exécuter.")
  return result
 return wrapper

def ouverture_gzip(chemin) : 
 with gzip.open(chemin, 'rb') as fichier_ouvert : 
  fichier_xml_gzip = fichier_ouvert.read()
  fichier_xml = fichier_xml_gzip.decode('latin-1')
  fichier_dict = xmltodict.parse(fichier_xml)
 return fichier_dict


def parse_budget(data_dict: dict) -> pd.DataFrame : 
 "Sépare les sous clefs lignes budget, sans nettoyage"
 ligne_budget = data_dict['DocumentBudgetaire']['Budget']['LigneBudget']
 df_ligne_budget = pd.DataFrame(ligne_budget)
 return df_ligne_budget

def parse_metadonnees(data_dict : dict) -> pd.DataFrame : 
 "Sépare les sous clefs de métadonnées, sans nettoyage"
 metadonnees = data_dict['DocumentBudgetaire']['EnTeteDocBudgetaire']
 df_metadonnees = pd.DataFrame(metadonnees)
 return df_metadonnees

def parse_schema(data_dict : dict) -> pd.DataFrame : 
 "Sépare la version schema sans nettoyage"
 version_schema = data_dict['DocumentBudgetaire']['VersionSchema']['@V']
 df_schema = pd.DataFrame({"VersionSchema": [version_schema]})
 return df_schema

def parse_date(data_dict : dict) -> pd.DataFrame : 
 date = data_dict['DocumentBudgetaire']['Budget']['BlocBudget']
 df_date = pd.DataFrame(date)
 return df_date

def assemblage(df_principal: pd.DataFrame, 
                         df_meta: pd.DataFrame, 
                         df_schem: pd.DataFrame,
                         df_date : pd.DataFrame) -> pd.DataFrame:
 """ Assemble les dataFrame contenant les metadonnees,
 la version schema et les lignes budgetaires """
 colonnes_a_conserver = ["VersionSchema", "DteDec", "LibelleColl",
                         "IdColl","Nature","LibCpte",
                         "Fonction","Operation",
                         "ContNat","ArtSpe",
                         "ContFon", "ContOp",
                         "CodRD","MtBudgPrec",
                         "MtRARPrec","MtPropNouv",
                         "MtPrev","CredOuv",
                         "MtReal","MtRAR3112",
                         "OpBudg","TypOpBudg",
                         "OpeCpteTiers"
                        ]
 
 df_final = pd.DataFrame(columns=colonnes_a_conserver)
 df_schem = pd.concat([df_schem] * len(df_principal), ignore_index=True)
 df_meta = pd.concat([df_meta] * len(df_principal), ignore_index=True)
 df_date = pd.concat([df_date] * len(df_principal), ignore_index=True)

 for col in df_schem.columns:
  if col in colonnes_a_conserver:
    df_final[col] = df_schem[col]

 for col in df_meta.columns:
  if col in colonnes_a_conserver:
    df_final[col] = df_meta[col]

 for col in df_principal.columns:
  if col in colonnes_a_conserver:
    df_final[col] = df_principal[col]

 for col in df_date.columns:
  if col in colonnes_a_conserver:
    df_final[col] = df_date[col]

 df_final = df_final.dropna(axis=1, how='all')
 return df_final

def nettoyage_lambda(df : pd.DataFrame) -> pd.DataFrame : 
 "Nettoie les données pour se débarasser des @V"
 nettoyage = lambda x : str(x).replace("{'@V': '", "").replace("'}", "")
 for col in df.columns : 
  df[col] = df[col].apply(nettoyage)
 return df 

def insertion_bdd(df_final: pd.DataFrame):
 """ insert dans une bdd les données maintenant transformées et en sort un csv à jour """
 chemin_bdd = os.path.join(DOSSIER_PARENT, BDD)
 conn = sqlite3.connect(chemin_bdd)
 df_final.to_sql('acte_budgetaire_gz', conn,
                    if_exists='append', index=False)
 
def creation_csv() :
 conn = sqlite3.connect(BDD)
 cursor = conn.cursor()
 info = cursor.execute('''Select * from acte_budgetaire_gz''')
 df = pd.DataFrame(info)
 fichier_csv = os.path.join(DOSSIER_PARENT, NOM_CSV)
 df.to_csv(fichier_csv, index=False)
 conn.close()

def deplacement_fichier(fichier_a_deplacer, dossier_destination):
 """ Déplace le fichier du dossier source au dossier fini"""
 chemin_source = pathlib.Path(fichier_a_deplacer)
 chemin_destination = pathlib.Path(dossier_destination) / chemin_source.name
 chemin_source.rename(chemin_destination)

@timing_decorator
def main():
 """ Traitement global, extrait et transforme les fichiers XML dans DOSSIER_SOURCE
    pour les insérer dans une bdd et en faire un csv"""
 liste_des_fichier_source = glob.glob(os.path.join(DOSSIER_SOURCE, "*.gz"))
 liste_des_fichier_traite = glob.glob(os.path.join(DOSSIER_SORTIE, "*.gz"))
 liste_des_df = []
 for fichier in liste_des_fichier_source:
  logging.info(f'Debut du travail sur {fichier}')
    # Sécurité permettant de ne pas injecter des doublons
  if fichier in liste_des_fichier_traite : 
   logging.error(f'Le fichier {fichier} a déjà été traité')
   return None 
  else : 
   data_dict = ouverture_gzip(fichier)
   if data_dict is not None:
    df_budget = parse_budget(data_dict)
    df_metadonnees = parse_metadonnees(data_dict)
    df_schema = parse_schema(data_dict)
    df_date = parse_date(data_dict)
    if df_budget is not None \
            and df_metadonnees is not None \
            and df_schema is not None \
            and df_date is not None:
     df_final = assemblage(
                df_budget, df_metadonnees, df_schema, df_date)
     liste_des_df.append(df_final)
     logging.info(f'Fin du travail sur {fichier}')
     deplacement_fichier(fichier, DOSSIER_SORTIE)
 df_session = pd.concat(liste_des_df, ignore_index = True)
 nettoyage_lambda(df_session)
 insertion_bdd(df_session)
 creation_csv()


if __name__ == "__main__":
    main()