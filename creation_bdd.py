import sqlite3

BDD = 'bdd_actes_budgetaires_gz.db'

def creation_bdd() : 
 conn = sqlite3.connect(BDD)
 cursor = conn.cursor()
 cursor.execute('''
  CREATE TABLE IF NOT EXISTS acte_budgetaire_gz ( 
    VersionSchema INT,
    DteDec DATE,
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
    TypOpBudg INT,
    OpeCpteTiers INT
                ) 
                ''')
 conn.commit()
 conn.close()


if __name__ == "__main__":
    creation_bdd()