import sqlite3

""" Crée les tables vierges dans lesquelles on va insérer les données voulues via les scripts etl_fichier_gz et script_transcodage"""
BDD = 'bdd_actes_budgetaires.db'

def creation_table_ligne_budget() : 
 # Ajoute des colonnes MtSup et CaracSup. 
 conn = sqlite3.connect(BDD)
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
 conn.commit()
 conn.close()
 
def creation_tables_transcodage() :
 ''' Créer les 5 tables de transcodage, 
 Code est considéré comme un texte pour que les 001 ne deviennent pas des 1'''
 conn = sqlite3.connect(BDD)
 cursor = conn.cursor()
 #Creation Table Nature
 cursor.execute('''
  CREATE TABLE IF NOT EXISTS Transcode_Nature (
   Code TEXT,       
   Lib_court TEXT, 
   Libelle TEXT,
   Section TEXT,
   Special INT,
   TypeChapitre TEXT,
   Nomenclature TEXT) ''')

 #Creation Table Cont Nat
 cursor.execute('''
  CREATE TABLE IF NOT EXISTS Transcode_Nature_Compte (
                Code TEXT, 
                DEquip TEXT,
                DOES TEXT,
                DOIS TEXT,
                DR TEXT,
                Lib_court TEXT,
                Libelle TEXT,
                REquip INT,
                ROES TEXT,
                ROIS TEXT,
                RR TEXT, 
                RegrTotalise TEXT,
                Supprime TEXT,
                SupprimeDepuis TEXT,
                Nomenclature TEXT
  )''') 

 #Creation Table Fonction
 cursor.execute('''
  CREATE TABLE IF NOT EXISTS Transcode_Fonction (
                Code TEXT,
                Lib_court	TEXT,
                Libelle	TEXT,
                Section	TEXT,
                Special	INT,
                TypeChapitre TEXT,
                Nomenclature TEXT
  )''')
 
 #Creation Table Fonction Compte

 cursor.execute('''
  CREATE TABLE IF NOT EXISTS Transcode_Fonction_Compte (
                Code TEXT,
                DEquip	TEXT,
                DOES	TEXT,
                DOIS	TEXT,
                DR	TEXT,
                Lib_court	TEXT,
                Libelle	TEXT,
                REquip	TEXT,
                ROES	TEXT,
                ROIS	TEXT,
                RR	TEXT,
                RegrTotalise	TEXT,
                Supprime	TEXT,
                SupprimeDepuis	TEXT,
                Nomenclature TEXT
  )''')
 
 #Creation Table Fonction Ref 
 cursor.execute('''
  CREATE TABLE IF NOT EXISTS Transcode_Fonction_RefFonctionnelles (
                Code TEXT,
                Lib_court	TEXT,
                Libelle	TEXT,
                Nomenclature TEXT
  )''')
 conn.commit()
 conn.close()


def creation_bdd() :
 creation_table_ligne_budget()
 creation_tables_transcodage()  

if __name__ == "__main__":
    creation_bdd()