import os
import sqlite3
import pathlib
import glob
import gzip 
import csv

import pandas as pd
import xmltodict

DOSSIER_PARENT = "."
DOSSIER_SOURCE = "./todo/"
FICHIERS_TODO = glob.glob(os.path.join(DOSSIER_SOURCE, "*.gz"))
BDD = 'bdd_actes_budgetaires.db'
FICHIER_CSV = 'donnees_lignes_budgets.csv'
COLONNES_A_CONSERVER = ['IdFichier', 'Nomenclature', 'DteStr',
                         'LibelleColl', 'IdColl', 'Nature',
                         'LibCpte', 'Fonction', 'Operation',
                         'ContNat', 'ArtSpe', 'ContFon',
                         'ContOp', 'CodRD', 'MtBudgPrec',
                         'MtRARPrec', 'MtPropNouv', 'MtPrev',
                         'CredOuv', 'MtReal', 'MtRAR3112',
                         'OpBudg', 'MtSup_1_Lib', 'MtSup_1_Val',
                         'MtSup_2_Lib', 'MtSup_2_Val', 'CaracSup_Lib',
                         'CaracSup_Val', 'TypOpBudg', 'OpeCpteTiers' 
                        ]

#Parsing et securite du fichier
def parse_fichier(chemin) : 
 '''Ouvre et parse le fichier gzip'''
 with gzip.open(chemin, 'rb') as fichier_ouvert : 
  fichier_xml_gzip = fichier_ouvert.read()
  fichier_xml = fichier_xml_gzip.decode('latin-1')
  fichier_dict = xmltodict.parse(fichier_xml)
 return fichier_dict

def _isolement_id(fichier) : 
 "Extrait l'id du nom du fichier pour la liste comprehension de securité"
 val_IdFichier = fichier.split("-")[-1].split('.')[0]
 return val_IdFichier

def _recherche_id_dans_bdd():
    conn = sqlite3.connect(BDD)
    cursor = conn.cursor()
    cursor.execute(''' SELECT DISTINCT IdFichier FROM actes_budgetaire ''')
    ligne_sql_int = [int(x[0]) for x in cursor.fetchall()]
    conn.close()
    return ligne_sql_int

#Extract Lignes Budget
def _transformation_MtSup(lignes_budget : dict) -> dict :
 '''Grâce aux spaghettis, il y a deux types de MtSup dans les fichiers,
   des dict et des listes de dict, permet de gérer les deux cas
 '''
 for i in lignes_budget :
  type_MtSup = i.get('MtSup') #Permet de connaitre le type de MtSup

  if isinstance(type_MtSup, dict) : 
   dict_MtSup = i['MtSup']
   i['MtSup_1_Lib'] = {'@V' : dict_MtSup['@Code']}
   i['MtSup_1_Val'] = {'@V' : dict_MtSup['@V']}

  elif isinstance(type_MtSup, list) :
   dict_MtSup = i['MtSup']
   mtsup_propre = {}

   for z, entry in enumerate(dict_MtSup, start=1):
    code = f'MtSup_{z}_Lib'
    valeur = f'MtSup_{z}_Val'
    mtsup_propre[code] = entry['@Code']
    mtsup_propre[valeur] = entry['@V'] 
   i.update(mtsup_propre)

  else : 
   pass  

 return lignes_budget

def _transformation_CaracSup(lignes_budget : dict) -> dict : 
 '''Grâce aux spaghettis, Caracsup a le même probleme que MtSup, gère ces cas'''
 for i in lignes_budget :
  if isinstance(i.get('CaracSup'), dict) :
   dict_CaracSup = i['CaracSup']
   i['CaracSup_Lib'] = {'@V' : dict_CaracSup['@Code']}
   i['CaracSup_Val'] = {'@V' : dict_CaracSup['@V']}
  else : 
   pass 
  return lignes_budget

def extract_lignes_budget(data_dict: dict) -> pd.DataFrame : 
 "Sépare les sous clefs lignes budget, sans nettoyage"
 ligne_budget = data_dict['DocumentBudgetaire']['Budget']['LigneBudget']
 ligne_mtsup_propre = _transformation_MtSup(ligne_budget)
 ligne_caracsup_mtsup_propre = _transformation_CaracSup(ligne_mtsup_propre)
 df_ligne_budget = pd.DataFrame(ligne_caracsup_mtsup_propre)
 df_ligne_budget.drop(columns = ['MtSup', 'CaracSup'])
 return df_ligne_budget

#Extract Metadonnees
def _extract_id(fichier) -> dict: 
 "Extrait l'id sous forme de dictionnaire pour le traitement"
 val_IdFichier = fichier.split("-")[-1].split('.')[0]
 IdFichier_dict = {"IdFichier" : val_IdFichier}
 return IdFichier_dict

def _extract_entete(data_dict : dict) -> dict : 
 '''Extrait l'entete docbudgetaire pour préaprer les métadonnées
 Extrait notamment les données de DteStr, LibelleColl et IdColl
 '''
 entetedoc_dict = data_dict['DocumentBudgetaire']['EnTeteDocBudgetaire']
 return entetedoc_dict

def _extract_nomenclature(data_dict : dict) -> dict : 
 '''La nomenclature se situe dans une autre branche, l'extrait individuellement'''
 Val_Nomenclature = data_dict['DocumentBudgetaire']['Budget']['EnTeteBudget']['Nomenclature']['@V']
 Nomenclature_dict = {"Nomenclature" : Val_Nomenclature}
 return Nomenclature_dict

def _assemblage_metadonnees(Id_Fichier : dict, entetedoc : dict, nomenclature : dict) -> pd.DataFrame :
 ''' Assemble les dictionnaires des divers métadonnées dans un DataFrame''' 
 colonnes_metadonnees = ['IdFichier', 'Nomenclature', 'DteStr', 'LibelleColl', 'IdColl']
 dict_metadonnees = {**Id_Fichier, **entetedoc, **nomenclature}
 df_metadonnees = pd.DataFrame(dict_metadonnees, index = [0])
 df_metadonnees = df_metadonnees[colonnes_metadonnees]
 return df_metadonnees

def extract_metadonnees(data_dict : dict, fichier) -> pd.DataFrame :
 ''' assemblage de plusieurs fonctions, objectif lisibilité'''
 IdFichier = _extract_id(fichier)
 Entete = _extract_entete(data_dict)
 Nomenclature = _extract_nomenclature(data_dict)
 df_metadonnees = _assemblage_metadonnees(IdFichier, Entete, Nomenclature)
 return df_metadonnees

#Creation du df final et nettoyage
def assemblage_metadonnees_et_lignes_budgets(df_lignes_budget, df_metadonnees) -> pd.DataFrame : 
 """ Assemble les dataFrame contenant les metadonnees et les lignes budgetaires """
 
 df_metadonnees = pd.concat([df_metadonnees] * len(df_lignes_budget), ignore_index=True)
 df_concat = pd.concat([df_metadonnees, df_lignes_budget], axis = 1)
 df_fichier = pd.DataFrame(columns=COLONNES_A_CONSERVER)

 for i in df_concat.columns : #Ne conserve que les colonnes qui nous intéressent,
  if i in COLONNES_A_CONSERVER :
   df_fichier[i] = df_concat[i]

 df_fichier = df_fichier.dropna(axis = 1, how = 'all')
 return df_fichier

def nettoyage_df(df : pd.DataFrame) -> pd.DataFrame : 
 "Nettoie les données pour se débarasser des @V"
 nettoyage = lambda x : str(x).replace("{'@V': '", "").replace("'}", "")
 for col in df.columns : 
  df[col] = df[col].apply(nettoyage)
 return df 

#Sortie du fichier
def insertion_bdd(df_final: pd.DataFrame):
 """ insert dans une bdd les données maintenant transformées et en sort un csv à jour """
 chemin_bdd = os.path.join(DOSSIER_PARENT, BDD)
 conn = sqlite3.connect(chemin_bdd)
 df_final.to_sql('actes_budgetaire', conn,
                    if_exists='append', index=False)

def insertion_csv_methode_session(df_final : pd.DataFrame) :
 ''' S'ajoute à un csv déjà existant, permet de ne pas créer un csv à chaque fois, n'utilise pas la bdd mais necessite un csv pré_existant'''
 with open(FICHIER_CSV, 'a', newline='') as fichier_csv : 
  ajout = csv.writer(fichier_csv)
  for _, ligne in df_final.iterrows():
   ajout.writerow(ligne)

def insertion_csv_methode_bdd() :
 ''' Crée une copie de la table actes_budgetaires sous forme de csv'''
 conn = sqlite3.connect(BDD)
 cursor = conn.cursor()
 info = cursor.execute('''SELECT * FROM actes_budgetaire''')
 df = pd.DataFrame(info)
 fichier_csv = os.path.join(DOSSIER_PARENT, FICHIER_CSV)
 df.to_csv(fichier_csv, index=False)
 conn.commit()
 conn.close()

def main_unitaire() : 
 Liste_fichiers_safe = [fichier for fichier in FICHIERS_TODO if int(_isolement_id(fichier)) not in _recherche_id_dans_bdd()]
 for fichier_safe in Liste_fichiers_safe :
  data_dict = parse_fichier(fichier_safe)
  df_lignes_budget = extract_lignes_budget(data_dict)
  df_metadonnees = extract_metadonnees(data_dict, fichier_safe)
  df_fichier_sale = assemblage_metadonnees_et_lignes_budgets(df_lignes_budget, df_metadonnees)
  df_fichier = nettoyage_df(df_fichier_sale)
  insertion_bdd(df_fichier)
 insertion_csv_methode_bdd()

def main_mi_unitaire_mi_global() :
 Liste_fichiers_safe = [fichier for fichier in FICHIERS_TODO if int(_isolement_id(fichier)) not in _recherche_id_dans_bdd()]
 Liste_df = []
 for fichier_safe in Liste_fichiers_safe :
  data_dict = parse_fichier(fichier_safe)
  df_lignes_budget = extract_lignes_budget(data_dict)
  df_metadonnees = extract_metadonnees(data_dict, fichier_safe)
  df_fichier_sale = assemblage_metadonnees_et_lignes_budgets(df_lignes_budget, df_metadonnees)
  Liste_df.append(df_fichier_sale)
 df_script_sale = pd.concat(Liste_df, ignore_index= True) 
 df_script = nettoyage_df(df_script_sale)
 insertion_bdd(df_script)
 insertion_csv_methode_bdd()

