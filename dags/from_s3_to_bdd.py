import os
import gzip
import glob
import json
import logging 
import xmltodict
import pandas as pd
import sqlalchemy
import pendulum
import boto3
from io import BytesIO

from sqlalchemy import MetaData, select, func, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError

from airflow import DAG
from datetime import datetime, timedelta 
from airflow.models import Connection, Variable
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

#log
logger = logging.getLogger(__name__)
logger.setLevel(logging.info)

#log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LISTE_COL_DOCUMENT_BUDGETAIRE = [['Id_Fichier','Nomenclature','Exer','Siret',
 'Siren','CodColl','DteStr','Date','DteDec','DteDecEx','NumDec','IdPost',
 'LibellePoste','LibelleColl','IdEtabPal','LibelleEtabPal','LibelleEtab','IdEtab',
 'NatDec','NatVote','OpeEquip','CodInseeColl','VoteFormelChap','TypProv','BudgPrec',
 'RefProv','ReprRes','NatFonc','PresentationSimplifiee','DepFoncN2','RecFoncN2',
 'DepInvN2','RecInvN2','CodTypBud','CodBud','ProjetBudget','Affect','SpecifBudget','FinJur',
 'Md5',
 'Sha1']]

LISTE_COL_SUPPRESSION_BUDGET = [
  'Code_nat_compte', 'Code_nat_chap', 'Code_fonc_compte', 'Code_mixte',
  'code_chap_mixte', "DEquip_nat_compte", 	"DOES_nat_compte", 	"DOIS_nat_compte", 	
  "DR_nat_compte", "REquip_nat_compte", 	"ROES_nat_compte", 	"ROIS_nat_compte" ,	"RR_nat_compte",
  'DEquip_nat_compte', 'DOES_nat_compte', 'DOIS_nat_compte', 'DR_nat_compte', 
  'REquip_nat_compte', 'ROES_nat_compte', 'ROIS_nat_compte', 'RR_nat_compte', 
  'RegrTotalise_nat_compte', 'Supprime_nat_compte', 'SupprimeDepuis_nat_compte', 
  'PourEtatSeul_nat_compte', 'PourEtatSeul_nat_chap', 'Section_nat_chap', 
  'Special_nat_chap', 'TypeChapitre_nat_chap', 'DEquip_fonc_compte', 
  'DOES_fonc_compte', 'DOIS_fonc_compte', 'DR_fonc_compte', 'REquip_fonc_compte', 
  'ROES_fonc_compte', 'ROIS_fonc_compte', 'RR_fonc_compte', 
  'RegrTotalise_fonc_compte', 'Supprime_fonc_compte', 'SupprimeDepuis_fonc_compte', 
  'Section_fonc_chap', 'Special_fonc_chap', 'TypeChapitre_fonc_chap' ]

LISTE_COL_MINIMALE_BUDGET = [
 'Id_Fichier','Nomenclature', 'Exer', 'TypOpBudg','Operation', 'Nature', 'ContNat',
 'LibCpte', 'Fonction', 'ContFon', 'ArtSpe', 'CodRD', 'MtBudgPrec', 'MtRARPrec',
 'MtPropNouv', 'MtPrev', 'OpBudg', 'CredOuv','MtReal', 'MtRAR3112', 'ContOp', 'OpeCpteTiers',
 'MtSup', 'MtSup_APVote', 'MtSup_Brut', 'MtSup_BudgetHorsRAR', 'MtSup_Comp', 'MtSup_ICNE', 'MtSup_ICNEPrec',
 'MtSup_MtOpeCumul', 'MtSup_MtOpeInfo', 'MtSup_Net', 'MtSup_MtPropNouv', 'MtSup_ProdChaRat', 'MtSup_RARPrec',
 'CaracSup', 'CaracSup_TypOpe', 'CaracSup_Section', 'CaracSup_ChapSpe',
  'CaracSup_ProgAutoLib', 'CaracSup_ProgAutoNum','CaracSup_VirCredNum', 'CaracSup_CodeRegion']

LIST_ANNEXES = ['DATA_AMORTISSEMENT_METHODE', 'DATA_APCP', 'DATA_AUTRE_ENGAGEMENT',
        'DATA_CHARGE', 'DATA_CONCOURS', 'DATA_CONSOLIDATION', 'DATA_CONTRAT_COUV', 
        "DATA_CONTRAT_COUV_REFERENCE", "DATA_CREDIT_BAIL", "DATA_DETTE", "DATA_EMPRUNT", 
        "DATA_ETAB_SERVICE", "DATA_FISCALITE", "DATA_FOND_AIDES_ECO", "DATA_FOND_COMM_HEBERGEMENT", 
        "DATA_FOND_EUROPEEN", "DATA_FOND_EUROPEEN_PROGRAMMATION", "DATA_FORMATION", 
        "DATA_FORMATION_PRO_JEUNES", "DATA_MEMBREASA", "DATA_ORGANISME_ENG", 
        "DATA_ORGANISME_GROUP", "DATA_PATRIMOINE", "DATA_PERSONNEL", "DATA_PERSONNEL_SOLDE", 
        "DATA_PPP", "DATA_PRET", "DATA_PROVISION", "DATA_RECETTE_AFFECTEE", 
        "DATA_SERVICE_FERROVIAIRE_BUD", "DATA_SERVICE_FERROVIAIRE_PATRIM", 
        "DATA_SERVICE_FERROVIAIRE_TER", "DATA_SIGNATAIRE", "DATA_SIGNATURE", 
        "DATA_SOMMAIRE", "DATA_TIERS", "DATA_TRESORERIE", "DATA_VENTILATION", "DATA_FLUX_CROISES"]

LISTE_COL_MAJ_BUDGET = [
    'MtSup_APVote', 'MtSup_Brut', 'MtSup_BudgetHorsRAR', 'MtSup_Comp', 
    'MtSup_ICNE', 'MtSup_ICNEPrec', 'MtSup_MtOpeCumul','MtSup_MtOpeInfo', 
    'MtSup_Net', 'MtSup_ProdChaRat', 'MtSup_RARPrec',
    'CaracSup_TypOpe', 'CaracSup_Section', 'CaracSup_ChapSpe',
    'CaracSup_ProgAutoLib', 'CaracSup_ProgAutoNum',
    'CaracSup_VirCredNum', 'CaracSup_CodeRegion']


def date_hier_str() : 
  ''' Crée un string pour le comparer aux dossiers dans le s3'''
  aujourd_hui = datetime.now()
  hier = aujourd_hui - timedelta(days=1)
  return hier.strftime("%Y%m%d/")


def extraction_id(fichier : str) -> str : 
  '''Extrait l'id du fichier, renvoie un str. '''

  val_id_fichier = fichier.split("/")[-1].split('.')[0]

  return val_id_fichier

def extraction_annexe(chemin_annexe, dict_metadonnees) :
  ''' Extrait les annexes pour les préparer à passer dans un dataframe''' 
  liste_annexe = []

  if isinstance(chemin_annexe, list) : 
    for row in chemin_annexe :
      if row is not None : 
        liste_par_ligne = {}
        for a, b in row.items() : 
          liste_par_ligne.update({a : b.get('@V')})
          liste_par_ligne.update(dict_metadonnees)
        liste_annexe.append(liste_par_ligne)

  elif isinstance(chemin_annexe, dict) : 
    dict_temp = {}
    for a, b in chemin_annexe.items() : 
      dict_temp.update({a : b.get('@V')})
      dict_temp.update(dict_metadonnees)
    liste_annexe.append(dict_temp)
  return liste_annexe 

def extraction_annexes_cfu(chemin_annexe , dict_metadonnees) -> list : 
  liste_annexe = []
  for row in chemin_annexe : 
   liste_par_ligne = {}
   for a, b in row.items() :
    if a not in ['@V','@calculated','@generator','@id'] :  
     try : 
      liste_par_ligne.update({a : b.get('@V')})
      liste_par_ligne.update(dict_metadonnees)
     except Exception as e : 
      print(row)
    elif a == '@V' : 
     liste_par_ligne.update({'nom_temp' : b})
    elif a in ['@calculated','@generator','@id'] : 
        liste_par_ligne.update(traitement_ligne_spe(row))
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

def traitement_ligne_spe(i) :
  ligne_spe = {} 
  for clef_ligne, val_ligne in i.items() : 
    if clef_ligne == 'MtSup' : 
        ligne_spe.update({clef_ligne : json.dumps(val_ligne)})
        type_m = i.get('MtSup')
        if isinstance(type_m, dict) : 
         ligne_spe.update({f"MtSup_{type_m.get('@Code')}" : type_m.get('@V')})
        elif isinstance(type_m, list) :
           for j in val_ligne : 
            ligne_spe.update({f"MtSup_{j.get('@Code')}" : j.get('@V')})

    elif clef_ligne == 'CaracSup' : 
        type_c = i.get('CaracSup')
        ligne_spe.update({clef_ligne : json.dumps(val_ligne)}) 
        if isinstance(type_c, dict) :
         ligne_spe.update({f"CaracSup_{type_c.get('@Code')}" : type_c.get('@V')})
        elif isinstance(type_c, list) : 
           for j in val_ligne : 
            ligne_spe.update({f"CaracSup_{j.get('@Code')}" : j.get('@V')})

    else :   
      if isinstance(val_ligne, dict) : 
        ligne_spe.update({clef_ligne : val_ligne.get('@V')}) 
      else :
        ligne_spe.update({clef_ligne : val_ligne})
  return ligne_spe

def extraction_lignes_budget_liste_cfu(chemin : list, dict_id : dict) -> list :
  ''' Explicite '''
  liste_budget = []
  liste_spe = []
  for lignes in chemin :
   dict_ligne = {}
   dict_ligne.update(dict_id)
   for a, b in lignes.items() :
      if a not in ['MtSup', 'CaracSup', '@calculated','@generator','@id'] : 
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

      elif a in ['@calculated','@generator','@id'] : 
        liste_spe.append(traitement_ligne_spe(lignes))
   liste_budget.append(dict_ligne)
  return liste_budget, liste_spe  

def extraction_budget_cfu(fichier_parse, infos_doc_budgetaire) : 
  lignes_budget = fichier_parse['DocumentBudgetaire']['Budget']['LigneBudget']

  liste_budget, liste_spe = extraction_lignes_budget_liste_cfu(lignes_budget, infos_doc_budgetaire)
  df_budget = pd.DataFrame(liste_budget)
  df_agglo = pd.DataFrame(liste_spe)
  df_agglo = df_agglo.rename({'@calculated' : 'calculated'})
  df_agglo = df_agglo.drop(columns = ['@generator','@id'])
  for key, val in infos_doc_budgetaire.items() : 
    df_agglo[key] = val
  return df_budget, df_agglo  

def jointure_libelle_agregats(df_agregat, val_natfonc, engine) : 
  val_nomenclature = df_agregat.iloc[0]['Nomenclature']
  val_exer = df_agregat.iloc[0]['Exer']

  df_nature_chap = pd.read_sql(f''' SELECT "Code_chapitre","Libelle_chapitre", "Exer","Nomenclature"
                                FROM nature_chapitre 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_fonction_chap = pd.read_sql(f''' SELECT "Code_fonction","Libelle_fonction","Exer","Nomenclature" 
                                FROM fonction_chapitre 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  if val_natfonc == '1' : 
    df_agregat = df_agregat.merge(df_nature_chap, 
                    left_on = ['Exer','Nomenclature','ChapitreNature'],
                    right_on = ['Exer','Nomenclature','Code_chapitre'], 
                    how = 'left')
    df_agregat = df_agregat.drop(columns = ['Code_chapitre'])
  elif val_natfonc in ['2','3'] : 
    df_agregat = df_agregat.merge(df_nature_chap, 
                    left_on = ['Exer','Nomenclature','ChapitreNature'],
                    right_on = ['Exer','Nomenclature','Code_chapitre'], 
                    how = 'left')
    df_agregat = df_agregat.merge(df_fonction_chap, 
                    left_on = ['Exer','Nomenclature','ChapitreFonction'],
                    right_on = ['Exer','Nomenclature','Code_fonction'], 
                    how = 'left')
    df_agregat = df_agregat.drop(columns = ['Code_chapitre', 'Code_fonction'])

  return df_agregat 


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

  dict_info_gen = {}
  if fichier_parse['DocumentBudgetaire']['Budget']['InformationsGenerales'] is not None : 
   chemin_info_gen = fichier_parse['DocumentBudgetaire']['Budget']['InformationsGenerales']['Information']
   if isinstance(chemin_info_gen, dict) : 
    if chemin_info_gen['@Code'] == 'EPCI' : 
      chemin_info_gen['@Code'] = 'EPCI_info_gen'
    dict_info_gen.update({chemin_info_gen['@Code'] : chemin_info_gen['@V']})
   elif isinstance(chemin_info_gen, list) : 
     for i in chemin_info_gen : 
       if '@V' not in i : 
        i['@V'] = '0'
       if i['@Code'] == 'EPCI' : 
         i['@Code'] = 'EPCI_info_gen'
       dict_info_gen.update({i['@Code'] : i['@V']})
  else : 
    None 

          

  liste_fichier = [{**blocbudget, **entetedocbudg, **entetebudget, 
                  **scellement, **dictionnaire_id, **dict_info_gen}]
  df_doc_budgetaire = pd.DataFrame(liste_fichier)
  colonnes_suppr = df_doc_budgetaire.columns.intersection(['NatCEPL', 'Departement'])
  df_doc_budgetaire = df_doc_budgetaire.drop(columns=colonnes_suppr)
  df_doc_budgetaire = df_doc_budgetaire.rename(columns={'IdColl' : 'Siret' })
  df_doc_budgetaire['Siren'] = df_doc_budgetaire['Siret'].str.slice(0,9)
  return df_doc_budgetaire

def verification_presence_id_table(id_fichier : str, engine, class_bloc_budget) : 
  '''Renvoie le nombre de fois où Id_Fichier est dans la base'''
  conn = engine.connect()
  try : 
    count = select(func.count("*")).select_from(class_bloc_budget).where(class_bloc_budget.Id_Fichier == id_fichier)
    requete = conn.execute(count).fetchone()
  finally : 
    conn.close()
  return requete 

def transcodage_bloc_old(dataframe_bloc, dictionnaire_transcodage) : 
  'Exploite le dictionnaire de données déjà sous format dict'
  for col in dataframe_bloc.columns : 
    if col in dictionnaire_transcodage.keys() : 
      dataframe_bloc[col] = dataframe_bloc[col].replace(dictionnaire_transcodage.get(col))
  return dataframe_bloc

def transcodage_bloc(dataframe_bloc, dictionnaire_transcodage):
    'Exploite le dictionnaire de données déjà sous format dict'
    for col in dataframe_bloc.columns:
        if col in dictionnaire_transcodage.keys():
            col_transcodage = dictionnaire_transcodage.get(col)
            if isinstance(col_transcodage, dict):
                dataframe_bloc[col] = dataframe_bloc[col].map(col_transcodage)
            else:
                dataframe_bloc[col] = dataframe_bloc[col].replace(col_transcodage)
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

def _jointure_libelle_nature_chap(df : pd.DataFrame,
                                df_nature_chap : pd.DataFrame) -> pd.DataFrame :
  ''' Necessite l'existence d'une colonne opération, qui est normalement optionnelle'''
  df['Code_chapitre'] = None
  df['Type_operation'] = None
  for index, row in df.iterrows():
      if row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'Code_chapitre'] = row['DOES_nat_compte']
        df.at[index, 'Type_operation'] = 'DOES'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'Code_chapitre'] = row['ROES_nat_compte']
        df.at[index, 'Type_operation'] = 'ROES'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'Code_chapitre'] = row['DOIS_nat_compte']
        df.at[index, 'Type_operation'] = 'DOIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'Code_chapitre'] = row['ROIS_nat_compte']
        df.at[index, 'Type_operation'] = 'ROIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'Code_chapitre'] = row['RR_nat_compte']
        df.at[index, 'Type_operation'] = 'RR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'Code_chapitre'] = row['DR_nat_compte']
        df.at[index, 'Type_operation'] = 'DR' 

  df['Code_chapitre'] = df['Code_chapitre'].astype(str)
  df = df.merge(df_nature_chap, 
                left_on = ['Exer','Nomenclature','Code_chapitre'],
                right_on = ['Exer','Nomenclature','Code_chapitre'],
                how = 'left')
  return df

def _jointure_libelle_fonction_chap(df : pd.DataFrame, 
                        df_fonction_chap : pd.DataFrame) -> pd.DataFrame : 
  ''' Necessite l'existence d'une colonne opération, qui est normalement optionnelle'''
  df['Code_fonction'] = None
  for index, row in df.iterrows():
      if row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'Code_fonction'] = row['DOES_fonc_compte']
        df.at[index, 'Type_fonction'] = 'DOES'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'Code_fonction'] = row['ROES_fonc_compte']
        df.at[index, 'Type_fonction'] = 'ROES'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'Code_fonction'] = row['DOIS_fonc_compte']
        df.at[index, 'Type_fonction'] = 'DOIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'Code_fonction'] = row['ROIS_fonc_compte']
        df.at[index, 'Type_fonction'] = 'ROIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'Code_fonction'] = row['RR_fonc_compte']
        df.at[index, 'Type_fonction'] = 'RR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'Code_fonction'] = row['DR_fonc_compte']
        df.at[index, 'Type_fonction'] = 'DR' 

  df['Code_fonction'] = df['Code_fonction'].astype(str)
  df = df.merge(df_fonction_chap, 
                left_on = ['Exer','Nomenclature','Code_fonction'],
                right_on = ['Exer','Nomenclature','Code_fonction'],
                how = 'left')
  return df

def _jointure_fonction(df_budget : pd.DataFrame, 
                              val_NatFonc : str, 
                              df_fonction_compte : pd.DataFrame, 
                              df_fonction_chap : pd.DataFrame, 
                              df_fonction_mixte : pd.DataFrame) -> pd.DataFrame :
  ''' Permet d'automatiser les jonctions des fonction WIP '''
  if val_NatFonc == '1' : 
    None 
  elif val_NatFonc == '2' : 
    df_budget = df_budget.merge(df_fonction_compte,
                  left_on = ['Nomenclature','Exer','Fonction'],
                  right_on = ['Nomenclature','Exer','Code_fonc_compte'],
                  how = 'left')
    df_budget = _jointure_libelle_fonction_chap(df_budget, df_fonction_chap)

  elif val_NatFonc == '3' :
    df_budget = df_budget.merge(df_fonction_mixte,
                  left_on = ['Nomenclature','Exer','Fonction'],
                  right_on = ['Nomenclature','Exer','Code_mixte'],
                  how = 'left')
    df_budget = df_budget.merge(df_fonction_chap,
                  left_on = ['Nomenclature','Exer','code_chap_mixte'], 
                  right_on = ['Nomenclature','Exer','Code_fonction'], 
                  how = 'left')

  return df_budget 

def _jointure_nature(df_budget : pd.DataFrame,
                  df_nature_chap : pd.DataFrame,
                  df_nature_compte : pd.DataFrame) -> pd.DataFrame : 

  df_budget = df_budget.merge(df_nature_compte,
                  left_on = ['Nomenclature','Exer','Nature'],
                  right_on = ['Nomenclature','Exer','Code_nat_compte'],
                  how = 'left')
  df_budget = _jointure_libelle_nature_chap(df_budget, df_nature_chap)
  return df_budget 


def creation_colonne_op_equip(df_budget: pd.DataFrame):
    '''Permet de savoir s'il s'agit d'une dépense ou recette d'équipement'''
    
    if 'Operation' in df_budget.columns:
        for index, ligne in df_budget.iterrows():
            if pd.notna(ligne['Operation']):
                df_budget.at[index, 'op_equip'] = 1
            else:
                df_budget.at[index, 'op_equip'] = 0
    else:
        df_budget['op_equip'] = 0 

    df_budget['op_equip'] = df_budget['op_equip'].astype(bool)
    return df_budget

def jointure_libelle_comptable(df_budget : pd.DataFrame, 
                        LISTE_COL_SUPPRESSION_BUDGET,
                        valeur_NatFonc, 
                        val_nomenclature, 
                        val_exer, 
                        engine) -> pd.DataFrame :

  df_nature_compte = pd.read_sql(f''' 
                                SELECT "Code_nat_compte", "DEquip_nat_compte", "DOES_nat_compte", "DOIS_nat_compte", 
                                  "DR_nat_compte", "Libelle_nature_compte", 	"REquip_nat_compte", 	
                                  "ROES_nat_compte" ,	"ROIS_nat_compte" ,	"RR_nat_compte", "Exer", 	"Nomenclature"
                                FROM nature_compte 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_nature_chap = pd.read_sql(f''' 
                                SELECT "Code_chapitre", 
                                  "Libelle_chapitre", "Section", "Exer", "Nomenclature"
                                FROM nature_chapitre 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_fonction_compte = pd.read_sql(f''' 
                                  SELECT "Exer", "Nomenclature", "Code_fonc_compte",
                                    "DEquip_fonc_compte", "DOES_fonc_compte", "DOIS_fonc_compte", 
                                    "DR_fonc_compte", "Libelle_rubrique_fonction", "REquip_fonc_compte", 	
                                    "ROES_fonc_compte" ,	"ROIS_fonc_compte", "RR_fonc_compte" 	
                                
                                  FROM fonction_compte 
                                  WHERE "Nomenclature" = '{val_nomenclature}'
                                   AND "Exer" = '{val_exer}' ''', engine)

  df_fonction_chap = pd.read_sql(f''' 
                                SELECT "Exer", "Nomenclature", "Code_fonction", 
                                  "Libelle_fonction" 
                                FROM fonction_chapitre 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_fonction_mixte = pd.read_sql(f''' 
                                SELECT "Exer", "Nomenclature", "Code_fonc_compte", "Libelle_rubrique_fonction", 
                                  "Code_mixte", "code_chap_mixte" FROM fonction_compte_mixte 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_budget = _jointure_nature(df_budget = df_budget, 
                            df_nature_chap = df_nature_chap,
                            df_nature_compte = df_nature_compte)
  df_budget = _jointure_fonction(df_budget, 
                            df_fonction_chap = df_fonction_chap,
                            df_fonction_compte = df_fonction_compte,
                            df_fonction_mixte = df_fonction_mixte, 
                            val_NatFonc = valeur_NatFonc
                            )

  df_budget = creation_colonne_op_equip(df_budget)

  for nom in LISTE_COL_SUPPRESSION_BUDGET : 
    if nom in df_budget.columns : 
      df_budget = df_budget.drop(columns = [nom])
  return df_budget

def creation_df_anomalies(df_budget: pd.DataFrame, dict_id: dict) -> pd.DataFrame:
  ''' Permet de mettre en lumière les différentes anomalies,
  pour le moment : fonction sans correspondances 
  
  Necessite un envoi to_sql et la création de la table associée'''

  
  if 'Libelle_fonc_compte' in df_budget.columns :
   sous_df = df_budget[(df_budget['Libelle_fonc_compte'].isna()) & (~df_budget['Fonction'].isna())]
  
   if sous_df.shape[0] == 0 : 
     None 
   elif sous_df.shape[0] > 0 : 
    df_anomalies = pd.DataFrame([dict_id])
    fonction_sans_ref = sous_df['Fonction'].drop_duplicates().to_list()
    nb_fonc_sans_ref = len(fonction_sans_ref)
    data = pd.Series([fonction_sans_ref], index=['fonctions_sans_ref'])
    df_fonc_sans_ref = pd.DataFrame(data).transpose()
    df_anomalies = pd.concat([df_anomalies, df_fonc_sans_ref], axis = 1)
    df_anomalies['nb_fonctions'] = nb_fonc_sans_ref
    df_anomalies['pourcentage'] = round(sous_df.shape[0] / df_budget.shape[0] * 100, 2)
        
    return df_anomalies

def ajout_data_siren(df_document_budgetaire, engine) : 
  ''' Jointure selective avec info siret
  Fonctionne s'il n'y a pas de correspondance'''
  val_siret = df_document_budgetaire.loc[0,'Siret']
  df_siret = pd.read_sql(f''' SELECT "siret", "CODGEO", "LIBGEO", "DEP", "REG", 
                                     "EPCI", "NATURE_EPCI", "libelleCategorieJuridique", 
                                     "denominationUniteLegale" 
                              FROM info_siret 
                              WHERE "siret" = '{val_siret}' 
                              LIMIT 1''',engine )
  df_doc_complet = df_document_budgetaire.merge(df_siret, left_on = 'Siret', 
                                                          right_on = 'siret', 
                                                          how = 'left')
  df_doc_complet = df_doc_complet.drop(columns = ['siret'])

  return df_doc_complet

def detect_date_format(date_str):
    try:
        pd.to_datetime(date_str, format='%Y-%m-%d')
        return '%Y-%m-%d'
    except ValueError:
        try:
            pd.to_datetime(date_str, format='%d-%m-%Y')
            return '%d-%m-%Y'
        except ValueError:
            return None

def convert_date(date_str):
    if date_str is None:
        return None
    if detect_date_format(date_str) == '%d-%m-%Y':
        return pd.to_datetime(date_str, format='%d-%m-%Y').strftime('%Y-%m-%d')
    elif detect_date_format(date_str) == '%Y-%m-%d':
        return date_str
    else:
        return None

def traitement_doc_budg(fichier_parse, dict_id, dico_transco, class_document_budgetaire, engine ) : 
  df_doc_budget = extraction_document_budgetaire(fichier_parse, dictionnaire_id= dict_id)
  df_doc_budget = transcodage_bloc(df_doc_budget, dico_transco)
  df_doc_budget = mise_au_norme_document_budgetaire(df_doc_budget, class_doc_budg = class_document_budgetaire)
  df_doc_budget = ajout_data_siren(df_doc_budget, engine)
  if "Date" in df_doc_budget.columns : 
    df_doc_budget = df_doc_budget.rename(columns = {'Date' : 'Date_acte'}) 
  return df_doc_budget

def traitement_bloc_budg(fichier_parse, dict_metadonnees, val_natfonc,
                         val_nomenclature, val_exer, engine, dico_transco) : 
  df_budget = extraction_budget(fichier_parse, dict_metadonnees)
  df_bloc_budget_vide = pd.DataFrame(columns=LISTE_COL_MINIMALE_BUDGET)
  df_budget = pd.concat([df_budget, df_bloc_budget_vide])
  df_budget = jointure_libelle_comptable(
                      df_budget = df_budget,
                      LISTE_COL_SUPPRESSION_BUDGET = LISTE_COL_SUPPRESSION_BUDGET, 
                      valeur_NatFonc = val_natfonc, 
                      val_nomenclature = val_nomenclature,
                      val_exer = val_exer, 
                      engine= engine
      )
  df_budget = transcodage_bloc(df_budget, dico_transco)
  df_budget = nettoyage_colonnes(df_budget, LISTE_COL_MAJ_BUDGET)
  df_budget = rename_bloc_budg(df_budget)
  return df_budget 

def rename_bloc_budg(df) :
  dict_colonnes = {'CodRD' : 'D_ou_R'} 
  for old_col, new_col in dict_colonnes.items() :
    if old_col in df.columns : 
      df = df.rename(columns = {old_col : new_col})
  return df 

#Ajouter les renames 
def traitement_bloc_budg_cfu(fichier_parse, dict_metadonnees, val_natfonc,
                         val_nomenclature, val_exer, engine, dico_transco) : 
  df_budget, df_agreg = extraction_budget_cfu(fichier_parse, dict_metadonnees)

  df_budget_pure = df_budget[~df_budget['Nature'].isna()]
  df_budget_sans_nat = df_budget[df_budget['Nature'].isna()]

  df_budget_pure = jointure_libelle_comptable(                      
                      df_budget = df_budget_pure,
                      LISTE_COL_SUPPRESSION_BUDGET = LISTE_COL_SUPPRESSION_BUDGET, 
                      valeur_NatFonc = val_natfonc, 
                      val_nomenclature = val_nomenclature,
                      val_exer = val_exer, 
                      engine= engine)
  
  df_agreg = jointure_libelle_agregats(
                      df_agregat = df_agreg, 
                      val_natfonc = val_natfonc, 
                      engine = engine )
  df_budget_sans_nat = jointure_libelle_agregats(
                      df_agregat = df_budget_sans_nat, 
                      val_natfonc = val_natfonc, 
                      engine = engine )

  df_budget_pure = transcodage_bloc(df_budget_pure, dico_transco)
  df_agreg = transcodage_bloc(df_agreg, dico_transco)
  df_budget_sans_nat = transcodage_bloc(df_budget_sans_nat, dico_transco)
  return df_budget_pure, df_agreg, df_budget_sans_nat

def traitement_acte_budgetaire(fichier_parse, engine, class_document_budgetaire, id_fichier, dico_transco) : 
  ''' S'occupe de la partie actes_budg, fichier parse sera différent selon le type de xml'''

  val_exer = fichier_parse['DocumentBudgetaire']['Budget']['BlocBudget']['Exer']['@V']
  val_nomenclature = fichier_parse['DocumentBudgetaire']['Budget']['EnTeteBudget']['Nomenclature']['@V']
  val_natfonc = fichier_parse['DocumentBudgetaire']['Budget']['BlocBudget']['NatFonc']['@V']

  dict_id = {'Id_Fichier' : id_fichier}
  dict_metadonnees = {'Id_Fichier' : id_fichier, 
                      'Nomenclature' : val_nomenclature,
                      'Exer' : val_exer}

  df_doc_budget = traitement_doc_budg(fichier_parse, dict_id, 
                                      dico_transco, class_document_budgetaire, 
                                      engine )

  df_budget = traitement_bloc_budg(fichier_parse, dict_metadonnees, val_natfonc,
                         val_nomenclature, val_exer, engine, dico_transco) 

  with engine.begin() as conn_alchemy : 
    df_doc_budget.to_sql('document_budgetaire', conn_alchemy ,if_exists = 'append', index = False, method = 'multi')
    df_budget.to_sql('bloc_budget', conn_alchemy ,if_exists = 'append', index = False, method = 'multi')

    chemin_general_annexe = fichier_parse['DocumentBudgetaire']['Budget']['Annexes']
    for data_annexe in LIST_ANNEXES :
      annexe_maj = data_annexe.split('DATA_')[1]
      data_annexe_min = data_annexe.lower()
      if data_annexe in chemin_general_annexe : 
        bloc_annexe = chemin_general_annexe[data_annexe][annexe_maj]
        df_annexe = pd.DataFrame(extraction_annexe(bloc_annexe, dict_metadonnees))
        df_annexe = transcodage_bloc(df_annexe, dico_transco)
        for i in ['DtEmission'] : 
          if i in df_annexe.columns : 
            if df_annexe[i].str.match(r'^\d{4}$').all():
                  df_annexe[i] = pd.to_datetime(df_annexe[i].astype(str) + '-01-01')
            else : 
             df_annexe[i] = df_annexe[i].replace('-', '/')
             try : 
              df_annexe[i] = pd.to_datetime(df_annexe[i], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
             except Exception as e :
              None 

        df_annexe.to_sql(data_annexe_min, conn_alchemy, if_exists = 'append', index = False, method = 'multi')



def traitement_acte_budgetaire_cfu(fichier_parse, engine, class_document_budgetaire, id_fichier, dico_transco) : 
  ''' S'occupe de la partie actes_budg, fichier parse sera différent selon le type de xml'''

  val_exer = fichier_parse['DocumentBudgetaire']['Budget']['BlocBudget']['Exer']['@V']
  val_nomenclature = fichier_parse['DocumentBudgetaire']['Budget']['EnTeteBudget']['Nomenclature']['@V']
  val_natfonc = fichier_parse['DocumentBudgetaire']['Budget']['BlocBudget']['NatFonc']['@V']

  dict_id = {'Id_Fichier' : id_fichier}
  dict_metadonnees = {'Id_Fichier' : id_fichier, 
                      'Nomenclature' : val_nomenclature,
                      'Exer' : val_exer}

  df_doc_budget = traitement_doc_budg(fichier_parse, dict_id, 
                                      dico_transco, class_document_budgetaire, 
                                      engine )

  df_budget, df_agglo = traitement_bloc_budg_cfu(fichier_parse, dict_metadonnees, val_natfonc,
                         val_nomenclature, val_exer, engine, dico_transco)                    


  with engine.begin() as conn_alchemy : 
    df_doc_budget.to_sql('document_budgetaire', conn_alchemy ,if_exists = 'append', index = False, method = 'multi')
    df_budget.to_sql('bloc_budget', conn_alchemy ,if_exists = 'append', index = False, method = 'multi')

    chemin_general_annexe = fichier_parse['DocumentBudgetaire']['Budget']['Annexes']
    for data_annexe in LIST_ANNEXES :
      annexe_maj = data_annexe.split('DATA_')[1]
      data_annexe_min = data_annexe.lower()
      if data_annexe in chemin_general_annexe : 
        bloc_annexe = chemin_general_annexe[data_annexe][annexe_maj]
        df_annexe = pd.DataFrame(extraction_annexes_cfu(bloc_annexe, dict_metadonnees))
        df_annexe = transcodage_bloc(df_annexe, dico_transco)
        df_annexe.to_sql(data_annexe_min, conn_alchemy, if_exists = 'append', index = False, method = 'multi')


  df_anomalies = creation_df_anomalies(df_budget, dict_id)
  if isinstance(df_anomalies, pd.DataFrame) :
    df_anomalies.to_sql('anomalies', engine ,if_exists = 'append', index = False, method = 'multi')
  else : 
    None

def extraction_document_comptable(xml_parse, dict_meta_3) : 
  ''' Isole et transforme les données qui seront insérées dans la table document_comptable'''

  chemin_info = xml_parse['doccfu:CompteFinancierUnique']['DocumentComptable']['EnteteComptable']['Infos']
  chemin_budget = xml_parse['doccfu:CompteFinancierUnique']['DocumentComptable']['BudgetComptable']

  dict_meta_doc = dict_meta_3.copy()

  for balise in chemin_info : 
    if balise not in ['Gestion', 'Exercice', 'NatFonc', 'NatVot', 'Nomenclature'] :
      temp_dict = {}
      for key, val in chemin_info[balise].items() : 
        nouvelle_key = key.replace('@','')
        temp_dict[nouvelle_key] = val
      dict_meta_doc.update(temp_dict)
    elif balise in ['NatFonc','NatVot'] :
      dict_meta_doc.update({balise : chemin_info[balise]['@V']}) 
    else : 
      None

  for i in chemin_budget : 
    if i != 'LigneComptable' : 
      dict_meta_doc.update({i.replace('@','') : chemin_budget[i]})

  return pd.DataFrame([dict_meta_doc])

def extraction_gestion_comptable(xml_parse, id_fichier) :
  ''' Extraction des données pour la table gestion'''
  try : 
    chemin_gestion = xml_parse['doccfu:CompteFinancierUnique']['DocumentComptable']['EnteteComptable']['Infos']['Gestion']
    if isinstance(chemin_gestion, list) : 
      df_gestion = pd.DataFrame(chemin_gestion)
    elif isinstance(chemin_gestion, dict) : 
      df_gestion = pd.DataFrame([chemin_gestion])
    df_gestion['Id_Fichier'] = id_fichier

    for col in df_gestion.columns : 
      df_gestion = df_gestion.rename(columns= {col : col.replace('@','')})
    return df_gestion

  except Exception :
    return None  

def extraction_ligne_comptable(xml_parse, dict_meta_3) : 
  ''' Extrait le bloc lignes comptables '''
  chemin_lcomptable = xml_parse['doccfu:CompteFinancierUnique']['DocumentComptable']['BudgetComptable']['LigneComptable']
  if isinstance(chemin_lcomptable, list) : 
    liste_ligne = []
    for i in chemin_lcomptable :
      dict_temp = {}
      for key, val in i.items() : 
        dict_temp.update({key : val.get('@V')})
      dict_temp.update(dict_meta_3)
      liste_ligne.append(dict_temp)
    df_l_compta =  pd.DataFrame(liste_ligne) 

  elif isinstance(chemin_lcomptable, dict) : 
    dict_mono_ligne = {}
    for key, val in chemin_lcomptable.items() : 
      dict_mono_ligne.update({key : val.get('@V')})
      dict_mono_ligne.update(dict_meta_3)
    df_l_compta =  pd.DataFrame([dict_mono_ligne])

  return df_l_compta

def traitement_cfu(xml_parse, engine, id_fichier) :
  ''' traite les balises cfu ''' 
  
  val_nomenclature = xml_parse['doccfu:CompteFinancierUnique']['DocumentBudgetaire']['Budget']['EnTeteBudget']['Nomenclature']['@V']
  val_exer = xml_parse['doccfu:CompteFinancierUnique']['DocumentBudgetaire']['Budget']['BlocBudget']['Exer']['@V']
  dict_meta = {'Nomenclature' : val_nomenclature,
                    'Exer' : val_exer } 
  dict_meta_comp = {'Nomenclature' : val_nomenclature,
                    'Exer' : val_exer,
                    'Id_Fichier' : id_fichier}
  df_document_comptable = extraction_document_comptable(xml_parse = xml_parse, 
                                                        dict_meta_3 = dict_meta_comp)
  df_gestion_comptable = extraction_gestion_comptable(xml_parse, id_fichier)
  df_bloc_comptable = extraction_ligne_comptable(xml_parse= xml_parse, 
                                                 dict_meta_3 = dict_meta_comp)
                                        
  with engine.begin() as conn_alchemy : 
    df_document_comptable.to_sql('document_comptable', conn_alchemy, if_exists = 'append', index=False, method = 'multi')
    df_document_comptable.to_sql('bloc_comptable', conn_alchemy, if_exists = 'append', index=False, method = 'multi')
    if isinstance(df_gestion_comptable, pd.DataFrame) : 
        df_gestion_comptable.to_sql('gestion_comptable', conn_alchemy, if_exists = 'append', index=False, method = 'multi')

def extraction_id_s3(fichier : str) -> str : 
  '''Extrait l'id du fichier, renvoie un str. '''

  val_id_fichier = fichier.split("/")[1].split('.')[0]

  return val_id_fichier

def xml_parse_to_bdd(xml_parse, string_file) : 
  
  db_creds = get_postgres_credentials(Variable.get("db_conn_id"))
  engine = sqlalchemy.create_engine(f"cockroachdb://{db_creds['POSTGRES_USERNAME']}:{db_creds['POSTGRES_PASSWORD']}@{db_creds['POSTGRES_HOST']}:{db_creds['POSTGRES_PORT']}/{db_creds['POSTGRES_DB']}?sslmode=require", echo=False)
  
  metadata = MetaData()
  metadata.reflect(engine)

  Base = declarative_base()
  class class_document_budgetaire(Base) :
     __table__ = Table('document_budgetaire', Base.metadata, autoload_with = engine)
     
  id_fichier = extraction_id_s3(string_file)
  df_transco = pd.read_sql('Select * from transcodage', engine)
  dico_transco = {j['nom_champ']: eval(j['enum']) for i, j in df_transco.iterrows()}

  comptage_presence = verification_presence_id_table(id_fichier, 
                                                      engine, 
                                                      class_document_budgetaire)
 
  if comptage_presence[0] > 0 : 
    logger.info(f'{id_fichier}, déjà présent dans la base de données')
    pass  
  elif comptage_presence[0] == 0 :    
    if 'doccfu:CompteFinancierUnique' in xml_parse :
      logger.info(id_fichier, 'cfu') 
      fichier_parse = xml_parse['doccfu:CompteFinancierUnique']
      traitement_cfu(xml_parse = xml_parse, engine = engine, id_fichier = id_fichier)
      traitement_acte_budgetaire_cfu(fichier_parse = fichier_parse, engine = engine, 
                               class_document_budgetaire = class_document_budgetaire, 
                               id_fichier = id_fichier, dico_transco = dico_transco)
    else : 
     logger.info(id_fichier, 'acte')
     traitement_acte_budgetaire(fichier_parse = xml_parse, engine = engine, 
                               class_document_budgetaire = class_document_budgetaire, 
                               id_fichier = id_fichier, dico_transco = dico_transco)



@task
def liste_file_from_s3():

    s3_creds_dlk = get_s3_credentials(Variable.get("s3-dev-input-dgcl"))

    bucket_name = 'actes-budgetaires'
    date_traitement = date_hier_str()

    session = boto3.Session(
        aws_access_key_id= s3_creds_dlk["aws_access_key_id"],
        aws_secret_access_key= s3_creds_dlk["aws_secret_access_key"],
        )
    s3 = session.client('s3', verify = False,
                        endpoint_url = s3_creds_dlk["endpoint_url"])

    response = s3.list_objects(Bucket=bucket_name, Prefix= date_traitement)
    
    liste_file = []

    if 'Contents' in response:
      for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.xml.gz'):
            liste_file.append(key)

    else : 
      logger.info("Pas de données aujoud'hui")
    return liste_file

@task
def traitement_fichier(chemin) : 

    s3_creds_dlk = get_s3_credentials(Variable.get("s3-dev-input-dgcl"))
    bucket_name = 'actes-budgetaires'

    session = boto3.Session(
        aws_access_key_id= s3_creds_dlk["aws_access_key_id"],
        aws_secret_access_key= s3_creds_dlk["aws_secret_access_key"],
        )
    s3 = session.client('s3', verify = False,
                        endpoint_url = s3_creds_dlk["endpoint_url"])

    response = s3.get_object(Bucket=bucket_name, Key=chemin)
    contenu = response['Body'].read()
    contenu_decompresse = gzip.GzipFile(fileobj=BytesIO(contenu)).read()
    fichier_dict = xmltodict.parse(contenu_decompresse)

    xml_parse_to_bdd(xml_parse= fichier_dict, string_file= chemin)

with DAG(dag_id="from_s3_to_bdd_local",
        description = "Recupere les fichiers J-1 sur le s3, les traite et les envoies dans la bdd",
        schedule_interval="@once",      #"@daily"
        tags=['actes_budgetaires', 'dev', 'daily','s3', 'coackroach', 'wip']
        ) as dag:
        
   task_1 = traitement_fichier.expand(file=liste_file_from_s3())
  
   task_1   
