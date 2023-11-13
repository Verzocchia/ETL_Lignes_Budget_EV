import sqlite3

""" Crée les tables vierges dans lesquelles on va insérer les données voulues via les scripts etl_fichier_gz et script_transcodage"""
BDD = 'bdd_actes_budgetaires.db'

def creation_table_ligne_budget(conn = 'connection') : 
 cursor = conn.cursor()
 cursor.execute('''
  CREATE TABLE IF NOT EXISTS actes_budgetaire ( 
    IdFichier TEXT,
    Nomenclature TEXT,
    DteStr DATE,
    LibelleColl TEXT,
    IdColl INT,
    Nature INT,
    LibCpte TEXT,
    Fonction INT,
    Operation INT,
    ContNat INT,
    ArtSpe BOOLEAN,
    ContFon INT,
    ContOp INT,
    CodRD TEXT,
    MtBudgPrec INT,
    MtRARPrec INT,
    MtPropNouv REAL,
    MtPrev REAL,
    CredOuv REAL,
    MtReal REAL,
    MtRAR3112 INT,
    OpBudg INT,
    MtSup_1_Lib TEXT,
    MtSup_1_Val REAL,
    MtSup_2_Lib TEXT,
    MtSup_2_Val REAL,
    MtSup_3_Lib TEXT,
    MtSup_3_Val REAL,         
    CaracSup_1_Lib TEXT,
    CaracSup_1_Val REAL,   
    CaracSup_2_Lib TEXT,
    CaracSup_2_Val REAL,            
    CaracSup_3_Lib TEXT,
    CaracSup_3_Val REAL,   
    TypOpBudg INT,
    OpeCpteTiers INT
                ) 
                ''')
 

def creation_table_transcodage_unique(conn = 'connection') : 
 ''' Crée la table unique de transcodage,
  -Code est considéré comme un texte pour que les 001 ne deviennent pas des 1
  -Colonne "Categorie" ajoutée, contient soit Nature, Nature_compte, Fonction, Fonction_compte, Fonction_rompte_ref''' 
 cursor = conn.cursor()
 cursor.execute('''
  CREATE TABLE IF NOT EXISTS Transcodage (
   Code TEXT,       
   Lib_court TEXT, 
   Libelle TEXT,
   Section TEXT,
   Special INT,
   TypeChapitre TEXT,
   DEquip TEXT,
   REquip INT,
   DOES TEXT,
   DOIS TEXT,
   DR TEXT,
   ROES TEXT,
   ROIS TEXT,
   RR TEXT, 
   RegrTotalise TEXT,
   Supprime TEXT,
   SupprimeDepuis TEXT,
   Nomenclature TEXT           
                ) 
                ''')

 

def creation_bdd() :
 connection = sqlite3.connect(BDD)
 creation_table_ligne_budget(conn = connection)
 creation_table_transcodage_unique(conn = connection)  
 connection.commit()
 connection.close()

if __name__ == "__main__":
    creation_bdd()