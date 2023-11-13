import os
import sqlite3
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
                         'MtSup_2_Lib', 'MtSup_2_Val', 'MtSup_3_Lib',
                         'MtSup_3_Val','CaracSup_1_Lib', 'CaracSup_1_Val', 
                         'CaracSup_2_Lib', 'CaracSup_2_Val', 'CaracSup_3_Lib',
                         'CaracSup_3_Val', 'TypOpBudg','OpeCpteTiers' 
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
 val_id_fichier = fichier.split("-")[-1].split('.')[0]
 return val_id_fichier

def _recherche_id_dans_bdd():
    conn = sqlite3.connect(BDD)
    cursor = conn.cursor()
    cursor.execute(''' SELECT DISTINCT IdFichier FROM actes_budgetaire ''')
    ligne_sql_int = [int(x[0]) for x in cursor.fetchall()]
    conn.close()
    return ligne_sql_int

#Extract Lignes_budget
def _transformation_MtSup(lignes_budget: dict) -> dict:
 '''Grâce aux spaghettis, il y a deux types de MtSup dans les fichiers,
       des dict et des listes de dict, permet de gérer les deux cas
    '''
 for i in lignes_budget:
  type_mtsup = i.get('MtSup')  # Permet de connaitre le type de MtSup
  if type_mtsup is not None:  # Vérifie si la clé 'MtSup' existe
    if isinstance(type_mtsup, dict):
      dict_mtsup = type_mtsup
      i['MtSup_1_Lib'] = {'@V': dict_mtsup.get('@Code', '')}
      i['MtSup_1_Val'] = {'@V': dict_mtsup.get('@V', '')}
    elif isinstance(type_mtsup, list):
      dict_mtsup = i['MtSup']
      mtsup_propre = {}
      for z, entry in enumerate(dict_mtsup, start=1):
        code = f'MtSup_{z}_Lib'
        valeur = f'MtSup_{z}_Val'
        mtsup_propre[code] = entry.get('@Code', '')
        mtsup_propre[valeur] = entry.get('@V', '')
        i.update(mtsup_propre)
        
 return lignes_budget

def _transformation_CaracSup(lignes_budget : dict) -> dict : 
 '''Grâce aux spaghettis, il y a deux types de MtSup dans les fichiers,
   des dict et des listes de dict, permet de gérer les deux cas
 '''
 for i in lignes_budget :
  type_carac_sup = i.get('CaracSup') #Permet de connaitre le type de MtSup

  if isinstance(type_carac_sup, dict) : 
   dict_carac_sup = i['CaracSup']
   i['CaracSup_1_Lib'] = {'@V' : dict_carac_sup['@Code']}
   i['CaracSup_1_Val'] = {'@V' : dict_carac_sup['@V']}

  elif isinstance(type_carac_sup, list) :
   dict_carac_sup = i['CaracSup']
   carac_sup_propre = {}

   for z, entry in enumerate(dict_carac_sup, start=1):
    code = f'CaracSup_{z}_Lib'
    valeur = f'CaracSup_{z}_Val'
    carac_sup_propre[code] = entry['@Code']
    carac_sup_propre[valeur] = entry['@V'] 
   i.update(carac_sup_propre)

  else : 
   pass  

 return lignes_budget

def extract_lignes_budget(data_dict: dict) -> pd.DataFrame : 
 "Sépare les sous clefs lignes budget, sans nettoyage"
 ligne_budget = data_dict['DocumentBudgetaire']['Budget']['LigneBudget']
 ligne_mtsup_propre = _transformation_MtSup(ligne_budget)
 ligne_caracsup_mtsup_propre = _transformation_CaracSup(ligne_mtsup_propre)
 df_ligne_budget = pd.DataFrame(ligne_caracsup_mtsup_propre)
 return df_ligne_budget

#Extract Metadonnees
def _extract_id(fichier) -> dict: 
 "Extrait l'id sous forme de dictionnaire pour le traitement"
 val_id_fichier = fichier.split("-")[-1].split('.')[0]
 idfichier_dict = {"IdFichier" : val_id_fichier}
 return idfichier_dict

def _extract_entete(data_dict : dict) -> dict : 
 '''Extrait l'entete docbudgetaire pour préaprer les métadonnées
 Extrait notamment les données de DteStr, LibelleColl et IdColl
 '''
 entetedoc_dict = data_dict['DocumentBudgetaire']['EnTeteDocBudgetaire']
 return entetedoc_dict

def _extract_nomenclature(data_dict : dict) -> dict : 
 '''La nomenclature se situe dans une autre branche, l'extrait individuellement'''
 val_nomenclature = data_dict['DocumentBudgetaire']['Budget']['EnTeteBudget']['Nomenclature']['@V']
 nomenclature_dict = {"Nomenclature" : val_nomenclature}
 return nomenclature_dict

def _assemblage_metadonnees(id_fichier : dict, entetedoc : dict, nomenclature : dict) -> pd.DataFrame :
 ''' Assemble les dictionnaires des divers métadonnées dans un DataFrame''' 
 colonnes_metadonnees = ['IdFichier', 'Nomenclature', 'DteStr', 'LibelleColl', 'IdColl']
 dict_metadonnees = {**id_fichier, **entetedoc, **nomenclature}
 df_metadonnees = pd.DataFrame(dict_metadonnees)
 df_metadonnees = df_metadonnees[colonnes_metadonnees]
 return df_metadonnees

def extract_metadonnees(data_dict : dict, fichier) -> pd.DataFrame :
 ''' assemblage de plusieurs fonctions, objectif lisibilité'''
 id_fichier = _extract_id(fichier)
 entete = _extract_entete(data_dict)
 nomenclature = _extract_nomenclature(data_dict)
 df_metadonnees = _assemblage_metadonnees(id_fichier, entete, nomenclature)
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

def main_unitaire() : #La liste comprehension n'est pas unitaire, go faire ça individuellement
 liste_fichiers_safe = [fichier for fichier in FICHIERS_TODO if int(_isolement_id(fichier)) not in _recherche_id_dans_bdd()]
 for fichier_safe in liste_fichiers_safe :
  data_dict = parse_fichier(fichier_safe)
  df_lignes_budget = extract_lignes_budget(data_dict)
  df_metadonnees = extract_metadonnees(data_dict, fichier_safe)
  df_fichier_sale = assemblage_metadonnees_et_lignes_budgets(df_lignes_budget, df_metadonnees)
  df_fichier = nettoyage_df(df_fichier_sale)
  insertion_bdd(df_fichier)
 insertion_csv_methode_bdd()



if __name__ == "__main__":
    main_unitaire()