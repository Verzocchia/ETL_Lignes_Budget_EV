''' 
Fonctionnement :

Objectif : Extraire les données du bloc budget des fichiers xml et les envoyer dans un parquet,
tout en conservant les métadonnées (id du fichier etc.)

Methode : Produit un parquet par xml

'''



''' Axes d'amélioration : 
    - CaracSup
    - Traiter les données comme MtSup
    - Nettoyer en amont les données 
    - Potentiellement y intégrer les annexes ''' 

import polars as pl 
import gzip 
import pandas as pd 
import xmltodict
import glob
import os 
import time 
import polars as pl 
import duckdb
import shutil 
import pyarrow.parquet as pq
import pyarrow as pa
import traceback

CHEMIN_ENTREE = './path/to/dossier_contenant_les_xml/'
CHEMIN_SORTIE = './path/to/dossier_qui_contiendra_les_parquets/'

#parquet_schema_temp (beaucoup de str)

schema_parquet = pa.schema([
    ('Id_Fichier', pa.string()), #int64
    ('IdColl', pa.string()),
    ('DteStr', pa.string()), #string
    ('LibellePoste', pa.string()),
    ('IdPost', pa.string()),
    ('TypOpBudg', pa.string()), #des 2 et des 1
    ('OpeCpteTiers', pa.string()),
    ('NatCEPL', pa.string()),
    ('Departement', pa.string()), #On oublie pas les 2A,
    ('NatDec', pa.string()), #A terme, sera un pa.dicitonnary
    ('Exer', pa.float32()), #int16
    ('ProjetBudget', pa.string()), #bool mais à verif 
    ('NatVote', pa.string()), #dico aussi
    ('OpeEquip', pa.string()), #necessite recherche
    ('VoteFormelChap', pa.string()), #bool mais à verif 
    ('TypProv', pa.string()), #int ? 
    ('BudgPrec', pa.string()), #int ?
    ('ReprRes', pa.string()), #int ?
    ('NatFonc', pa.string()), #int ?
    ('CodTypBud', pa.string()),
    ('LibelleEtabPal', pa.string()),
    ('IdEtabPal', pa.string()),
    ('LibelleEtab', pa.string()),
    ('IdEtab', pa.string()),
    ('CodColl', pa.string()),
    ('CodInseeColl', pa.string()),
    ('CodBud', pa.string()),
    ('Nomenclature', pa.string()),
    ('Nature', pa.string()),
    ('Fonction', pa.string()),
    ('LibCpte', pa.string()),
    ('ContNat', pa.string()),
    ('ArtSpe', pa.string()), #Doit être corrigé en bool,
    ('ContFon', pa.string()),
    ('CodRD', pa.string()), #Val D ou R,
    ('MtBudgPrec', pa.float32()), #float32 #nullable = True
    ('MtRARPrec', pa.float32()), #float32
    ('MtPropNouv', pa.float32()), #float32
    ('MtPrev', pa.float32()), #float32
    ('CredOuv', pa.float32()), #int32
    ('MtReal', pa.float32()), #float32
    ('MtRAR3112', pa.float32()), #float32
    ('OpBudg', pa.string()),
    ('MtSup', pa.string()),
    ('CaracSup', pa.string()),
        ('BudgetHorsRAR', pa.string()),
        ('ICNE', pa.string()),
        ('ICNEPrec', pa.string()),
        ('APVote', pa.string()),
        ('ProdChaRat', pa.string()),
        ('RARPrec', pa.string()),
        ('ProgAutoLib', pa.string()),
        ('ProgAutoNum', pa.string()),
        ('MtOpeCumul', pa.string()),
        ('Brut', pa.string()),
        ('Comp', pa.string()),
        ('Net', pa.string()),
        ('ChapSpe', pa.string()),
        ('MtOpeInfo', pa.string()),
        ('TypOpe', pa.string())
    ])

#Fonctions de base

def _recherche_id_dans_parquet(chemin_parquet_db):
 
 conduck = duckdb.connect(database=':memory:', read_only=False)
 source_duck = conduck.read_parquet(f'{chemin_parquet_db}')
 requete = conduck.execute(''' 
    SELECT DISTINCT Id_Fichier 
    FROM source_duck''')
 ligne_sql_int = [int(x[0]) for x in requete.fetchall()]
 conduck.close()
 return ligne_sql_int

def parse_fichier(chemin) : 
 '''Ouvre et parse le fichier gzip'''
 with gzip.open(chemin, 'rb') as fichier_ouvert : 
  fichier_xml_gzip = fichier_ouvert.read()
  fichier_xml = fichier_xml_gzip.decode('latin-1')
  fichier_dict = xmltodict.parse(fichier_xml)
 return fichier_dict

def recherche_id_fichier(chemin_parquet) :
 ''' Pas encore utilisée, permet de récupérer les ID dans le parquet contenant les 
 données déjà traitées '''
 conduck = duckdb.connect(database=':memory:', read_only=False)
 docubudg_t = conduck.read_parquet(chemin_parquet)
 requete_duckdb = ''' 
 SELECT
    DISTINCT Id_Fichier
 FROM 
    docubudg_t
 '''
 result_requete= conduck.execute(requete_duckdb).fetchdf()
 liste_id = result_requete['Id_Fichier'].to_list()
 conduck.close()
 return liste_id

def _isolement_id(fichier) : 
 '''Extrait l'id du nom du fichier pour la liste comprehension de securité

 ATTENTION, le premier split / va changer si on l'applique sur du minio '''
 val_id_fichier_source = fichier.split("\\")[-1].split('.')[0]
 if '-' in val_id_fichier_source : 
  val_id_fichier = val_id_fichier_source.split('-')[1]
 else : 
  val_id_fichier= val_id_fichier_source
 return val_id_fichier

def nettoyage_V(dataframe : pd.DataFrame) -> pd.DataFrame :
 ''' Permet de supprimer les @V des colonnes à l'exception de MtSup et CaracSup'''

 nettoyage = lambda x : str(x).replace("{'@V': '", "").replace("'}", "")
 for col in dataframe.columns : 
  if col in ['MtSup', 'CaracSup'] : 
   dataframe[col] = dataframe[col].astype(str) 
  else :
   dataframe[col] = dataframe[col].apply(nettoyage)
 dataframe_propre = dataframe.reset_index(drop=True)
 return dataframe_propre

def travail_mtsup_ligne(ligne_budg) :
 type_m = ligne_budg.get('MtSup')
 if type_m is None : 
  pass 
 else : 
  if isinstance(type_m, dict) : 
   ligne_budg[type_m.get('@Code', '')] = {'@V' : type_m.get('@V', '')}
  elif isinstance(type_m, list) :
    for z in type_m : #z c'est un dict contenant un V et un Code 
      ligne_budg[z.get('@Code', '')] = {'@V' : z.get('@V', '')}
 return ligne_budg

def travail_caracsup_ligne(ligne_budg) : 
 type_c = ligne_budg.get('CaracSup')
 if type_c is None : 
  pass 
 else : 
  if isinstance(type_c, dict) : 
   ligne_budg[type_c.get('@Code', '')] = {'@V' : type_c.get('@V', '')}
  elif isinstance(type_c, list) : 
   for ii in type_c : 
    ligne_budg[ii.get('@Code', '')] = {'@V' : ii.get('@V', '')}
 return ligne_budg

def xml_to_parquet(chemin_xml_entree, chemin_sortie) : 
 #
 #REMPLACER chemin_xml_entree_glob par liste_fichiers_safe
 #
 #
 chemin_xml_entree_glob = glob.glob(os.path.join(chemin_xml_entree, "*.gz"))
 #liste_verif_parquet = _recherche_id_dans_parquet(chemin_parquet_db= chemin_parquet_verif)
 #liste_fichiers_safe = [chemin_fichier for chemin_fichier in chemin_xml_entree_glob if int(_isolement_id(chemin_fichier)) not in liste_verif_parquet]
 for fichier in chemin_xml_entree_glob : 
  try : 
   id_fichier = _isolement_id(fichier)
   dico = parse_fichier(fichier)
   metadonnees = dico['DocumentBudgetaire']['EnTeteDocBudgetaire']
   metadonnees.update(dico['DocumentBudgetaire']['Budget']['BlocBudget'])
   metadonnees.update(dico['DocumentBudgetaire']['Budget']['EnTeteBudget'])
   metadonnees['Id_Fichier'] = {'@V' : id_fichier}
   docbase = dico['DocumentBudgetaire']['Budget']['LigneBudget']
   if isinstance(docbase, list):
    for i in docbase : 
      i.update(metadonnees)
      i = travail_mtsup_ligne(i)
      i = travail_caracsup_ligne(i)
   elif isinstance(docbase, dict) : 
    docbase.update(metadonnees)
    docbase = travail_mtsup_ligne(docbase)
    docbase = travail_caracsup_ligne(docbase)
   df_base = pd.DataFrame(docbase)
   df_propre = nettoyage_V(df_base)
   df_propre.to_parquet(f'{chemin_sortie}{id_fichier}_parquet', engine='pyarrow')
   #shutil.move(fichier, './fichiers/done_xml/')
  except Exception as e : 
    print(f'Erreur fichier {fichier}, extraction impossible')
    #shutil.move(fichier, './fichiers/todo_xml/error/')
    print(f'Erreur complète : {repr(e)}')
    traceback.print_exc()
    continue 