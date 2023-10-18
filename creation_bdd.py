import sqlite3

BDD = 'bdd_actes_budgetaires.db'

def creation_bdd_et_csv() : 
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
 

if __name__ == "__main__":
    creation_bdd_et_csv()