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
  'Code_nat_compte', 'code_nat_chap', 'Code_nat_chap',
  'Code_fonc_compte','code_fonc_chap','Code_fonc_chap', 'Code_mixte',
  'code_chap_mixte']

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
        "DATA_FORMATION_PRO_JEUNES", "DATA_MEMBRESASA", "DATA_ORGANISME_ENG", 
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

def parse_fichier(chemin : str) -> dict: 
  '''Ouvre et parse le fichier gzip en dictionnaire'''

  with gzip.open(chemin, 'rb') as fichier_ouvert :

   fichier_xml_gzip = fichier_ouvert.read()
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

def verification_presence_id_table(id_fichier : str, engine, class_bloc_budget) : 
  '''Renvoie le nombre de fois où Id_Fichier est dans la base'''
  conn = engine.connect()
  try : 
    count = select(func.count("*")).select_from(class_bloc_budget).where(class_bloc_budget.Id_Fichier == id_fichier)
    requete = conn.execute(count).fetchone()
  finally : 
    conn.close()
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

def _jointure_libelle_nature_chap(df : pd.DataFrame,
                                df_nature_chap : pd.DataFrame) -> pd.DataFrame :
  ''' Necessite l'existence d'une colonne opération, qui est normalement optionnelle'''
  if 'Operation' in df.columns : 
    for index, row in df.iterrows():
      if row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_nat_chap'] = row['DOES_nat_compte']
        df.at[index, 'type_nature'] = 'DOES'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_nat_chap'] = row['ROES_nat_compte']
        df.at[index, 'type_nature'] = 'ROES'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_nat_chap'] = row[ 'DOIS_nat_compte']
        df.at[index, 'type_nature'] = 'DOIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_nat_chap'] = row[ 'ROIS_nat_compte']
        df.at[index, 'type_nature'] = 'ROIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.isna(row['Operation']):
        df.at[index, 'code_nat_chap'] = row['RR_nat_compte']
        df.at[index, 'type_nature'] = 'RR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.isna(row['Operation']):
        df.at[index, 'code_nat_chap'] = row['DR_nat_compte']
        df.at[index, 'type_nature'] = 'DR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.notna(row['Operation']):
        df.at[index, 'code_nat_chap'] = row['DEquip_nat_compte']
        df.at[index, 'type_nature'] = 'DEquip'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.notna(row['Operation']):
        df.at[index, 'code_nat_chap'] = row['REquip_nat_compte']
        df.at[index, 'type_nature'] = 'REquip'
  else :
    for index, row in df.iterrows():
      if row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_nat_chap'] = row['DOES_nat_compte']
        df.at[index, 'type_nature'] = 'DOES'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_nat_chap'] = row['ROES_nat_compte']
        df.at[index, 'type_nature'] = 'ROES'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_nat_chap'] = row['DOIS_nat_compte']
        df.at[index, 'val_prise'] = 'DOIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_nat_chap'] = row['ROIS_nat_compte']
        df.at[index, 'type_nature'] = 'ROIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'code_nat_chap'] = row['RR_nat_compte']
        df.at[index, 'type_nature'] = 'RR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'code_nat_chap'] = row['DR_nat_compte']
        df.at[index, 'type_nature'] = 'DR' 

  df['code_nat_chap'] = df['code_nat_chap'].astype(str)
  df = df.merge(df_nature_chap, 
                left_on = ['Exer','Nomenclature','code_nat_chap'],
                right_on = ['Exer','Nomenclature','Code_nat_chap'],
                how = 'left')
  return df

def _jointure_libelle_fonction_chap(df : pd.DataFrame, 
                        df_fonction_chap : pd.DataFrame) -> pd.DataFrame : 
  ''' Necessite l'existence d'une colonne opération, qui est normalement optionnelle'''
  if 'Operation' in df.columns : 
    for index, row in df.iterrows():
      if row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_fonc_chap'] = row['DOES_fonc_compte']
        df.at[index, 'type_fonction'] = 'DOES'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_fonc_chap'] = row['ROES_fonc_compte']
        df.at[index, 'type_fonction'] = 'ROES'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_fonc_chap'] = row[ 'DOIS_fonc_compte']
        df.at[index, 'type_fonction'] = 'DOIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_fonc_chap'] = row[ 'R_fonc_compte']
        df.at[index, 'type_fonction'] = 'ROIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.isna(row['Operation']):
        df.at[index, 'code_fonc_chap'] = row['RR_fonc_compte']
        df.at[index, 'type_fonction'] = 'RR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.isna(row['Operation']):
        df.at[index, 'code_fonc_chap'] = row['DR_fonc_compte']
        df.at[index, 'type_fonction'] = 'DR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.notna(row['Operation']):
        df.at[index, 'code_fonc_chap'] = row['DEquip_fonc_compte']
        df.at[index, 'type_fonction'] = 'DEquip'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) and pd.notna(row['Operation']):
        df.at[index, 'code_fonc_chap'] = row['REquip_fonc_compte']
        df.at[index, 'type_fonction'] = 'REquip'
  else :
    for index, row in df.iterrows():
      if row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_fonc_chap'] = row['DOES_fonc_compte']
        df.at[index, 'type_fonction'] = 'DOES'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '2':
        df.at[index, 'code_fonc_chap'] = row['ROES_fonc_compte']
        df.at[index, 'type_fonction'] = 'ROES'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_fonc_chap'] = row['DOIS_fonc_compte']
        df.at[index, 'type_fonction'] = 'DOIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '1' and row['TypOpBudg'] == '1':
        df.at[index, 'code_fonc_chap'] = row['ROIS_fonc_compte']
        df.at[index, 'type_fonction'] = 'ROIS'
      elif row['CodRD'] == 'R' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'code_fonc_chap'] = row['RR_fonc_compte']
        df.at[index, 'type_fonction'] = 'RR'
      elif row['CodRD'] == 'D' and row['OpBudg'] == '0' and pd.isna(row['TypOpBudg']) :
        df.at[index, 'code_fonc_chap'] = row['DR_fonc_compte']
        df.at[index, 'type_fonction'] = 'DR' 

  df['code_fonc_chap'] = df['code_fonc_chap'].astype(str)
  df = df.merge(df_fonction_chap, 
                left_on = ['Exer','Nomenclature','code_fonc_chap'],
                right_on = ['Exer','Nomenclature','Code_fonc_chap'],
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
                  right_on = ['Nomenclature','Exer','Code_fonc_chap'], 
                  how = 'left')
    df_budget.drop(columns = ['Section_fonc_chap', 'Special_fonc_chap'])

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

def jointure_libelle_comptable(df_budget : pd.DataFrame, 
                        LISTE_COL_SUPPRESSION_BUDGET,
                        valeur_NatFonc, 
                        val_nomenclature, 
                        val_exer, 
                        engine) -> pd.DataFrame :

  df_nature_compte = pd.read_sql(f''' SELECT * FROM nature_compte 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_nature_chap = pd.read_sql(f''' SELECT * FROM nature_chapitre 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_fonction_compte = pd.read_sql(f''' SELECT * FROM fonction_compte 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_fonction_chap = pd.read_sql(f''' SELECT * FROM fonction_chapitre 
                                WHERE "Nomenclature" = '{val_nomenclature}'
                                 AND "Exer" = '{val_exer}' ''', engine)

  df_fonction_mixte = pd.read_sql(f''' SELECT * FROM fonction_compte_mixte 
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

def traitement_doc_budg(fichier_parse, dict_id, dico_transco, class_document_budgetaire, engine ) : 
  df_doc_budget = extraction_document_budgetaire(fichier_parse, dictionnaire_id= dict_id)
  df_doc_budget = transcodage_bloc(df_doc_budget, dico_transco)
  df_doc_budget = mise_au_norme_document_budgetaire(df_doc_budget, class_doc_budg = class_document_budgetaire)
  df_doc_budget = ajout_data_siren(df_doc_budget, engine)
  
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
  return df_budget 

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
     #Try car les fichiers n'auront jamais toutes les annexes
     #Donc cette partie du script va tenter de traiter des val None comme des df, provoquant des erreurs
     try :  
      annexe_maj = data_annexe.split('DATA_')[1]
      data_annexe_min = data_annexe.lower()
      bloc_annexe = chemin_general_annexe[data_annexe][annexe_maj]
      df_annexe = pd.DataFrame(extraction_annexe(bloc_annexe, dict_metadonnees))
      df_annexe = transcodage_bloc(df_annexe, dico_transco)
      df_annexe.to_sql(data_annexe_min, conn_alchemy, if_exists = 'append', index = False, method = 'multi')
     except Exception as e : 
      None 

  df_anomalies = creation_df_anomalies(df_budget, dict_id)
  if isinstance(df_anomalies, pd.DataFrame) :
    df_anomalies.to_sql('anomalies', engine ,if_exists = 'append', index = False, method = 'multi')
  else : 
    None

def extraction_document_comptable(xml_parse, dict_meta_3) : 
  ''' Isole et transforme les données qui seront insérées dans la table document_comptable'''

  chemin_info = xml_parse['doccfu:CompteFinancierUnique']['DocumentComptable']['EnteteComptable']['Infos']
  chemin_budget = xml_parse['doccfu:CompteFinancierUnique']['DocumentComptable']['BudgetComptable']

  for balise in chemin_info : 
    if balise not in ['Gestion', 'Exercice', 'NatFonc', 'NatVot', 'Nomenclature'] :
      temp_dict = {}
      for key, val in chemin_info[balise].items() : 
        nouvelle_key = key.replace('@','')
        temp_dict[nouvelle_key] = val
      dict_meta_3.update(temp_dict)
    elif balise in ['NatFonc','NatVot'] :
      dict_meta_3.update({balise : chemin_info[balise]['@V']}) 
    else : 
      None

  for i in chemin_budget : 
    if i != 'LigneComptable' : 
      dict_meta_3.update({i.replace('@','') : chemin_budget[i]})

  return pd.DataFrame([dict_meta_3])

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
               'Exer' : val_exer}  
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

@task 
def dl_files_from_s3():

    s3_id = Variable.get("hub_coll_id")
    s3_mdp = Variable.get("hub_coll_secret")
    s3_endpoint = Variable.get("hub_coll_endpoint")

    bucket_name = 'actes-budgetaires'
    date_traitement = date_hier_str()

    session = boto3.Session(
        aws_access_key_id= s3_id,
        aws_secret_access_key= s3_mdp,
        )
    s3 = session.client('s3', verify = False,
                        endpoint_url = s3_endpoint)

    response = s3.list_objects(Bucket=bucket_name, Prefix= date_traitement)
    
    if 'Contents' in response:
      for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.xml.gz'):
            id_file = key.split('/')[1]
            print(key)
            chemin_local = os.path.join('./data_test/fichier_test_boto/', f'{id_file}.xml.gz')
            s3.download_file(bucket_name, key, chemin_local)

    else : 
      print("Pas de données aujoud'hui")

@task
def get_files() : 
  liste_file = glob.glob(os.path.join('./data_test/fichier_test_boto/', "*.gz"))
  return liste_file

@task                                       
def from_xml_to_bdd(file) : 
  
  db_creds = get_postgres_credentials(Variable.get("db_conn_id"))

  engine = ouverture_connection_postgres(Variable.get("pg_conn_id"))
  metadata = MetaData()
  metadata.reflect(engine)

  Base = declarative_base()
  class class_document_budgetaire(Base) :
     __table__ = Table('document_budgetaire', Base.metadata, autoload_with = engine)
     
  id_fichier = extraction_id(file)
  df_transco = pd.read_sql('Select * from transcodage', engine)
  dico_transco = {j['nom_champ']: eval(j['enum']) for i, j in df_transco.iterrows()}

  comptage_presence = verification_presence_id_table(id_fichier, 
                                                      engine, 
                                                      class_document_budgetaire)
 
  if comptage_presence[0] > 0 : 
    logger.INFO(f'{id_fichier}, déjà présent dans la base de données')
    pass  
  elif comptage_presence[0] == 0 :
    xml_parse = parse_fichier(file)
    
    if 'doccfu:CompteFinancierUnique' in xml_parse :
      print('cfu') 
      fichier_parse = xml_parse['doccfu:CompteFinancierUnique']
      traitement_cfu(xml_parse, id_fichier)
      traitement_acte_budgetaire(fichier_parse = fichier_parse, engine = engine, 
                               class_document_budgetaire = class_document_budgetaire, 
                               id_fichier = id_fichier, dico_transco = dico_transco)
    else : 
     print('acte')
     traitement_acte_budgetaire(fichier_parse = xml_parse, engine = engine, 
                               class_document_budgetaire = class_document_budgetaire, 
                               id_fichier = id_fichier, dico_transco = dico_transco)

with DAG(dag_id="from_s3_to_bdd_local",
        description = "Recupere les fichiers J-1 sur le s3, les traite et les envoies dans la bdd",
        schedule_interval="@once",      #"@daily"
        tags=['actes_budgetaires', 'dev', 'daily','s3', 'coackroach', 'wip']
        ) as dag:


   task_1 = dl_files_from_s3()
   task_2 = from_xml_to_bdd.expand(file=get_files())
  
   task_1 >> task_2