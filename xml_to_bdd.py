''' bdd actuelle : db_test_copy 
pas de __main__ etc. Car nécessite deux arguments : chemin des xml et chemin de dictionnaire_v2.csv'''


import os
import gzip
import glob
import json
import logging 
import xmltodict
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, select, func, Table
from sqlalchemy.orm import DeclarativeBase

#log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

engine = sqlalchemy.create_engine('postgresql://verzochia:verzochia@localhost:5432/db_v1')
conn = engine.connect()
metadata = MetaData()

LIST_ANNEXES = ['DATA_AMORTISSEMENT_METHODE', 'DATA_APCP', 'DATA_AUTRE_ENGAGEMENT',
        'DATA_CHARGE', 'DATA_CONCOURS', 'DATA_CONSOLIDATION', 'DATA_CONTRAT_COUV', 
        "DATA_CONTRAT_COUV_REFERENCE", "DATA_CREDIT_BAIL", "DATA_DETTE", "DATA_EMPRUNT", 
        "DATA_ETAB_SERVICE", "DATA_FISCALITE", "DATA_FOND_AIDES_ECO", "DATA_FOND_COMM_HEBERGEMENT", 
        "DATA_FOND_EUROPEEN", "DATA_FOND_EUROPEEN_PROGRAMMATION", "DATA_FORMATION", 
        "DATA_FORMATION_PRO_JEUNES", "DATA_MEMBRESASA", "DATA_ORGANISME_ENG", 
        "DATA_ORGANISME_GROUP", "DATA_PATRIMOINE", "DATA_PERSONNEL", "DATA_PERSONNEL_SOLDE", 
        "DATA_PPP", "DATA_PRET", "DATA_PROVISION", "DATA_RECETTE_AFFECTEE", 
        "DATA_SERVICE_FERROVIAIRE_BUD", "DATA_SERVICE_FERROVIAIRE_PATRIM", 
        "DATA_SERVICE_FERROVIAIRE_TER", "DATA_SIGNATAIRE", "DATA_SIGNATURE", 
        "DATA_SOMMAIRE", "DATA_TIERS", "DATA_TRESORERIE", "DATA_VENTILATION", "DATA_FLUX_CROISES"]

LISTE_COL_SUP = [
    'MtSup_APVote', 'MtSup_Brut', 'MtSup_BudgetHorsRAR', 'MtSup_Comp', 
    'MtSup_ICNE', 'MtSup_ICNEPrec', 'MtSup_MtOpeCumul','MtSup_MtOpeInfo', 
    'MtSup_Net', 'MtSup_ProdChaRat', 'MtSup_RARPrec',
    'CaracSup_TypOpe', 'CaracSup_Section', 'CaracSup_ChapSpe',
    'CaracSup_ProgAutoLib', 'CaracSup_ProgAutoNum',
    'CaracSup_VirCredNum', 'CaracSup_CodeRegion']

def extraction_id(fichier : str) -> str : 
  '''Extrait l'id du fichier, renvoie un str. '''

  val_id_fichier = fichier.split("/")[-1].split('.')[0]

  return val_id_fichier

def parse_fichier(chemin : str) -> dict: 
  '''Ouvre et parse le fichier gzip en dictionnaire'''

  with gzip.open(chemin, 'rb') as fichier_ouvert :

   fichier_xml_gzip = fichier_ouvert.read()
   fichier_xml = fichier_xml_gzip.decode('latin-1')
   fichier_dict = xmltodict.parse(fichier_xml)
  return fichier_dict

def extraction_annexe(chemin_annexe : dict, dict_metadonnees : dict) -> list :
  ''' Extrait les annexes pour les préparer à passer dans un dataframe''' 
  liste_annexe = []
  for row in chemin_annexe : 
   liste_par_ligne = {}
   for a, b in row.items() : 
    liste_par_ligne.update({a : b.get('@V')})
    liste_par_ligne.update(dict_metadonnees)
   liste_annexe.append(liste_par_ligne)
  return liste_annexe 

def extraction_donnees(chemin : dict) -> dict  : 
  ''' Permet de passer de {'@V' : 'XDZ', '@Code' : 'blab'} à {'blab' : 'XDZ'}'''
  dict_annexe = {}
  for a, b in chemin.items() : 
    dict_annexe.update({a : b.get('@V')})
  return dict_annexe 

def extraction_lignes_budget_liste(chemin : dict, dict_id : dict) -> list :
  ''' Explicite '''
  liste_budget = []
  for lignes in chemin : 
   dict_ligne = {}
   dict_ligne.update(dict_id)
   for a, b in lignes.items() :
      if a not in ['MtSup', 'CaracSup'] : 
        dict_ligne.update({a : b.get('@V')}) 
 
      elif a == 'MtSup' : 
        dict_ligne.update({a : json.dumps(b)})
        type_m = lignes.get('MtSup')
  
        if isinstance(type_m, dict) : 
         dict_ligne.update({f"MtSup_{type_m.get('@Code')}" : type_m.get('@V')})
  
        elif isinstance(type_m, list) :
           for j in b : 
            dict_ligne.update({f"MtSup_{j.get('@Code')}" : j.get('@V')})
  
      elif a == 'CaracSup' :   
        type_c = lignes.get('CaracSup')
        dict_ligne.update({a : json.dumps(b)}) 
 
        if isinstance(type_c, dict) :
         dict_ligne.update({f"CaracSup_{type_c.get('@Code')}" : type_c.get('@V')})
  
        elif isinstance(type_c, list) : 
           for j in b : 
            dict_ligne.update({f"CaracSup_{j.get('@Code')}" : j.get('@V')})
 
   liste_budget.append(dict_ligne)
  return liste_budget

def mise_au_norme_document_budgetaire(df, class_doc_budg) :
  ''' Verifie que le df n'ait que des colonnes qui sont dans la table document_budgetaire'''
  liste_to_suppr = []
  liste_col_table = list(class_doc_budg.__table__.columns.keys())

  for col in df.columns : 
    if col not in liste_col_table : 
      liste_to_suppr.append(col)

  df_au_norme = df.drop(columns=liste_to_suppr)
  if liste_to_suppr: 
      logger.info(f"Fichier {df['Id_Fichier'].iloc[0]}, 'col pas dans le schema :', {liste_to_suppr}")

  return df_au_norme

def extraction_budget(fichier_parse : dict , infos_doc_budgetaire : dict)  -> pd.DataFrame : 
  ''' Extrait toutes les données budgetaires, y compris carac et mtsup '''
  lignes_budget = fichier_parse['DocumentBudgetaire']['Budget']['LigneBudget'] 
 
  if isinstance(lignes_budget, dict) : 
   donnees_budget_prep = extraction_donnees(lignes_budget)
   donnees_budget_prep.update(infos_doc_budgetaire)
   donnees_budget = [donnees_budget_prep]

  elif isinstance(lignes_budget, list) : 
   donnees_budget = extraction_lignes_budget_liste(lignes_budget, infos_doc_budgetaire)
 
  df_budget = pd.DataFrame(donnees_budget)
  return df_budget 

def extraction_document_budgetaire(fichier_parse : dict, dictionnaire_id : dict) -> pd.DataFrame : 
  ''' Extrait les métadonnées du fichier pour la table document_budgetaire '''
  blocbudget = extraction_donnees(fichier_parse['DocumentBudgetaire']['Budget']['BlocBudget'])
  entetedocbudg = extraction_donnees(fichier_parse['DocumentBudgetaire']['EnTeteDocBudgetaire'])
  entetebudget = extraction_donnees(fichier_parse['DocumentBudgetaire']['Budget']['EnTeteBudget'])
  scellement = {'Date' : fichier_parse['DocumentBudgetaire']['Scellement'].get('@date'),
                'Md5' : fichier_parse['DocumentBudgetaire']['Scellement'].get('@md5'), 
                'Sha1' : fichier_parse['DocumentBudgetaire']['Scellement'].get('@sha1')}

  liste_fichier = [{**blocbudget, **entetedocbudg, **entetebudget, **scellement, **dictionnaire_id}]
  df_doc_budgetaire = pd.DataFrame(liste_fichier)
  colonnes_suppr = df_doc_budgetaire.columns.intersection(['NatCEPL', 'Departement'])
  df_doc_budgetaire = df_doc_budgetaire.drop(columns=colonnes_suppr)
  df_doc_budgetaire = df_doc_budgetaire.rename(columns={'IdColl' : 'Siret' })
  df_doc_budgetaire['Siren'] = df_doc_budgetaire['Siret'].str.slice(0,9)
  return df_doc_budgetaire

def verification_presence_id_table(id_fichier : str, conn, class_bloc_budget) : 
  '''Renvoie le nombre de fois où Id_Fichier est dans la base'''
  count = select(func.count("*")).select_from(class_bloc_budget).where(class_bloc_budget.Id_Fichier == id_fichier)
  requete = conn.execute(count).fetchone()
  return requete 

def transcodage_bloc(dataframe_bloc, dictionnaire_transcodage) : 
  'Exploite le dictionnaire de données déjà sous format dict'
  for col in dataframe_bloc.columns : 
    if col in dictionnaire_transcodage.keys() : 
      dataframe_bloc[col] = dataframe_bloc[col].replace(dictionnaire_transcodage.get(col))
  return dataframe_bloc

def nettoyage_colonnes(df_budget, liste_colonnes_propre) :
  ''' Remplace les MtSupRARprec etc. par MtSupRARPrec pour que ça rentre en bdd
  C'est pas une belle fonction, ça c'est sûr, mais c'est une rustine qui fonctionne'''
  dict_replace = {}
  for col_budget in df_budget.columns:
      col_budget_min = col_budget.lower()

      for col_propre in liste_colonnes_propre:
        col_propre_min = col_propre.lower()

        if col_budget_min == col_propre_min:
            dict_replace.update({col_budget : col_propre})

  df_budget = df_budget.rename(columns=dict_replace)
  return df_budget


def xml_to_bdd(chemin_des_xml, dico_transco_csv) :
  ''' Fonction principale : 
  Extrait les données des xml dans le dossier et les envoie dans la table'''

  chemin_xml_entree_glob = glob.glob(os.path.join(chemin_des_xml, "*.gz"))

  #Prep transco
  dico_transco_csv = pd.read_csv(dico_transco_csv)
  dico_transco = {j['nom_champ']: eval(j['enum']) for i, j in dico_transco_csv.iterrows()}

  #Prep verif integrité
  engine = sqlalchemy.create_engine('postgresql://verzochia:verzochia@localhost:5432/db_test_copy')
  conn = engine.connect()
  metadata = MetaData()
 
  class Base(DeclarativeBase):
     pass
 
  class document_budgetaire(Base) :
     __table__ = Table('document_budgetaire', Base.metadata, autoload_with = engine)

  for fichier in chemin_xml_entree_glob : 
   id_fichier = extraction_id(fichier)

   if id_fichier is None : 
    pass 

   else :
    comptage_presence = verification_presence_id_table(id_fichier, 
                                                      conn, 
                                                      document_budgetaire) 
    if comptage_presence[0] > 0 : 
       #print(f'Fichier {id_fichier} déjà extrait dans la base ! ')
       pass 
    elif comptage_presence[0] == 0 :
     try : 
      fichier_parse = parse_fichier(fichier)
      chemin_exer = fichier_parse['DocumentBudgetaire']['Budget']['BlocBudget']['Exer']
      chemin_nomenclature = fichier_parse['DocumentBudgetaire']['Budget']['EnTeteBudget']['Nomenclature']
      dict_metadonnees = {'Id_Fichier' : id_fichier, 
                         'Nomenclature' : chemin_nomenclature.get('@V'),
                         'Exer' : chemin_exer.get('@V')}
      dict_id = {'Id_Fichier' : id_fichier}

      #extraction et insertion doc budget (table centrale)
      df_doc_budget = extraction_document_budgetaire(fichier_parse, dictionnaire_id= dict_id)
      df_doc_budget = transcodage_bloc(df_doc_budget, dico_transco)
      df_doc_budget = mise_au_norme_document_budgetaire(df_doc_budget, class_doc_budg = document_budgetaire)
      df_doc_budget.to_sql('document_budgetaire', engine ,if_exists = 'append', index = False, method = 'multi')

      #df_budget
      df_budget = extraction_budget(fichier_parse, dict_metadonnees)
      df_budget = transcodage_bloc(df_budget, dico_transco)
      df_budget = nettoyage_colonnes(df_budget, LISTE_COL_SUP)

      df_budget.to_sql('bloc_budget', engine ,if_exists = 'append', index = False, method = 'multi')

      #Annexes 
      chemin_general_annexe = fichier_parse['DocumentBudgetaire']['Budget']['Annexes']
      for data_annexe in LIST_ANNEXES : 
       annexe_maj = data_annexe.split('DATA_')[1]
       data_annexe_min = data_annexe.lower()
       try :
        bloc_annexe = chemin_general_annexe[data_annexe][annexe_maj]
        df_annexe = pd.DataFrame(extraction_annexe(bloc_annexe, dict_metadonnees))
        df_annexe = transcodage_bloc(df_annexe, dico_transco)
        df_annexe.to_sql(data_annexe_min, engine, if_exists = 'append', index = False, method = 'multi')
       except Exception as e : 
         None 
         
     except Exception as e : 
       logger.error(f"Error processing file {id_fichier}: {e}")

