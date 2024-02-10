# Objectif : Créer des tables vides

# 1 : Supprime les tables si elles existaient dans le cas d'un reset
# 2 : Crée les nouvelles tables

import psycopg2
from sqlalchemy import create_engine
from psycopg2 import sql


TABLE_BLOC_BUDGET_QUERY = """
    CREATE TABLE Bloc_Budget (
        ID SERIAL PRIMARY KEY,
        Id_Fichier INT NOT NULL,
        Nomenclature VARCHAR,
        Exer INT,
        TypOpBudg INT,
        Operation VARCHAR,
        Nature VARCHAR,
        ContNat VARCHAR,
        LibCpte VARCHAR,
        Fonction VARCHAR,
        ContFon VARCHAR,
        ArtSpe BOOLEAN,
        CodRD VARCHAR,
        MtBudgPrec FLOAT,
        MtRARPrec FLOAT,
        MtPropNouv FLOAT,
        MtPrev FLOAT,
        OpBudg BOOLEAN,
        CredOuv FLOAT,
        MtRAR3112 FLOAT,
        MtReal FLOAT,
        ContOp VARCHAR,
        OpeCpteTiers VARCHAR,
        MtSup VARCHAR,
        APVote FLOAT,
        Brut FLOAT,
        BudgetHorsRAR FLOAT,
        Comp VARCHAR,
        ICNE FLOAT,
        ICNEPrec FLOAT,
        MtOpeCumul FLOAT,
        MtOpeInfo FLOAT,
        Net FLOAT,
        ProdChaRat FLOAT,
        RARPrec FLOAT,
        CaracSup VARCHAR,
        TypOpe INT,
        Section VARCHAR,
        ChapSpe VARCHAR,
        ProgAutoLib VARCHAR,
        ProgAutoNum VARCHAR,
        VirCredNum VARCHAR,
        CodeRegion INT
    )"""

TABLE_DOC_BUDGET_QUERY = '''  CREATE TABLE Doc_Budget  (
    Id_Fichier INT UNIQUE NOT NULL,
    Nomenclature VARCHAR,
    Exer SMALLINT,
    Siret BIGINT,
    Siren BIGINT,
    CodColl VARCHAR,
    LibelleColl VARCHAR,
    DteStr TIMESTAMP,
    date_precise TIMESTAMP WITH TIME ZONE,
    DteDec TIMESTAMP,
    DteDecEx TIMESTAMP,
    NumDec VARCHAR,
    IdPost VARCHAR,
    LibellePoste VARCHAR,
    LibelleEtabPal VARCHAR,
    IdEtabPal BIGINT,
    LibelleEtab VARCHAR,
    IdEtab BIGINT,
    NatDec VARCHAR,
    NatVote VARCHAR,
    OpeEquip BOOLEAN,
    CodInseeColl VARCHAR,
    VoteFormelChap BOOLEAN,
    TypProv SMALLINT,
    BudgPrec SMALLINT,
    RefProv VARCHAR,
    ReprRes SMALLINT,
    NatFonc SMALLINT,
    PresentationSimplifiee BOOLEAN,
    DepFoncN2 FLOAT,
    RecFoncN2 FLOAT,
    DepInvN2 FLOAT,
    RecInvN2 FLOAT,
    CodTypBud VARCHAR,
    CodBud VARCHAR,
    ProjetBudget BOOLEAN,
    Affect VARCHAR,
    SpecifBudget BIGINT,
    FinJur BIGINT,
    md5 VARCHAR,
    sha1 VARCHAR )'''

TABLE_ETABLISSEMENT_QUERY = ''' CREATE TABLE Etablissement (
 Siret INTEGER UNIQUE NOT NULL,
 Siren INTEGER, 

 codeCommuneEtablissement VARCHAR, 
 CODGEO VARCHAR, 
 LIBGEO VARCHAR, 
 DEP VARCHAR,
 REG VARCHAR,
 EPCI VARCHAR,
 
 libelleCommuneEtablissement VARCHAR,
 denominationUniteLegale VARCHAR,  
 libelleCategorieJuridique VARCHAR,
 
 categorieJuridiqueUniteLegale INTEGER, 
 sigleUniteLegale VARCHAR, 
 dateCreationUniteLegale TIMESTAMP
 )'''

TABLE_ANNEXE_CHARGE_QUERY = ''' CREATE TABLE Data_Charge (
 ID SERIAL PRIMARY KEY,
 Id_Fichier INTEGER, 
 Exer_Doc INTEGER,
 Exer INTEGER,
 CodTypeCharge VARCHAR,
 NatDepTransf VARCHAR,
 DureeEtal INTEGER, 
 DtDelib TIMESTAMP,
 MtDepTransf FLOAT,
 MtAmort FLOAT,
 MtDotAmort FLOAT, 
 Champ_Editeur VARCHAR
 ) ''' 

TABLE_ANNEXE_CONCOURS_QUERRY =  ''' CREATE TABLE Data_concours ( 
 ID SERIAL PRIMARY KEY,
 Id_Fichier INTEGER,
 Exer_doc INTEGER, 
 CodArticle VARCHAR, 
 CodInvFonc VARCHAR, 
 CodNatJurBenefCA VARCHAR, 
 LibOrgaBenef VARCHAR,
 MtSubv FLOAT, 
 LibPrestaNat VARCHAR, 
 DenomOuNumSubv VARCHAR,  
 NatDec VARCHAR, 
 ObjSubv VARCHAR, 
 Siret VARCHAR,  
 PopCommune INTEGER,
 Champ_Editeur VARCHAR 
 )'''


TABLES = [TABLE_BLOC_BUDGET_QUERY,
        TABLE_DOC_BUDGET_QUERY,
        TABLE_ETABLISSEMENT_QUERY,
        TABLE_ANNEXE_CHARGE_QUERY,
        TABLE_ANNEXE_CONCOURS_QUERRY ]




def supression_table(conn) : 
 ''' Supprimes les tables doc_budget, bloc_budget, etablissement, 
    data_charge, data_concours'''   
 try : 
    cursor = conn.cursor()
    cursor.execute(''' 
    DROP TABLE IF EXISTS Doc_Budget;
    DROP TABLE IF EXISTS Bloc_Budget;
    DROP TABLE IF EXISTS Etablissement;
    DROP TABLE IF EXISTS Data_Charge;
    DROP TABLE IF EXISTS Data_Concours;
    ''')
    conn.commit()

 except psycopg2.Error as e :
    print('Table non supprimée')
    print(e)
    conn.rollback()

 finally :
    cursor.close() 

def creation_table(conn, requete_table) :
    try : 
        cursor = conn.cursor()
        cursor.execute(requete_table)
        conn.commit()

    except psycopg2.Error as e :
        print('Table non crée, raison : ')
        print(e)
        conn.rollback()

    finally :
        cursor.close() 

def main() : 
    ''' Crée les tables, supprimes les précédentes SI elles existaient'''
    con = psycopg2.connect(dbname='db_v1',
        user= 'postgres',
        host='',
        port = '5432')

    supression_table(conn = con)

    for requete_table in TABLES :
        creation_table(con, requete_table)