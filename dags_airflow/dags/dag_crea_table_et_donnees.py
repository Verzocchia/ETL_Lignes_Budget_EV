import os
import pandas as pd 
from datetime import datetime


import sqlalchemy 
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, MetaData, Table, DateTime, Float, Boolean 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

import airflow 
from airflow import DAG 
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import Connection, Variable

def ouverture_connection_postgres(connection_id):
    conn = Connection.get_connection_from_secrets(connection_id)
    return sqlalchemy.create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    

@task.branch(task_id="crea_tables")
def creation_tables() : 
 ''' Utilises la syntaxe sqlalchemy 1.4 '''

 engine = ouverture_connection_postgres(Variable.get("pg_conn_id"))
 Base = declarative_base()

 class document_budgetaire(Base) :
  __tablename__ = 'document_budgetaire' 
  Id_Fichier = Column(String, primary_key= True)
  Nomenclature = Column(String)
  Exer = Column(Integer)
  Siret = Column(String, nullable = True)
  Siren = Column(String, nullable = True)
  CodColl = Column(String)
  DteStr = Column(DateTime, nullable= True)
  Date = Column(DateTime(timezone=True), nullable= True)
  DteDec = Column(DateTime, nullable= True)
  DteDecEx = Column(DateTime, nullable= True)

  NumDec = Column(String, nullable = True)
  IdPost = Column(String, nullable = True)
  LibellePoste = Column(String, nullable = True)
  LibelleColl = Column(String, nullable = True)
  IdEtabPal = Column(String, nullable = True)
  LibelleEtabPal = Column(String, nullable = True)
  LibelleEtab = Column(String)
  IdEtab = Column(String, nullable = True)
  NatDec = Column(Integer)
  NatVote = Column(String, nullable = True)
  OpeEquip = Column(Boolean, nullable = True)
  CodInseeColl = Column(String, nullable = True)
  VoteFormelChap = Column(Boolean, nullable = True)
  TypProv = Column(Integer, nullable = True)
  BudgPrec = Column(Integer)
  RefProv = Column(String, nullable = True)
  ReprRes = Column(Integer)
  NatFonc = Column(Integer)

  CODGEO = Column(String, nullable = True)
  LIBGEO = Column(String, nullable = True)
  DEP = Column(String, nullable = True)
  REG = Column(String, nullable = True)
  EPCI = Column(String, nullable = True)
  NATURE_EPCI = Column(String, nullable = True)
  libelleCategorieJuridique = Column(String, nullable = True)  
  denominationUniteLegale = Column(String, nullable = True)

  PresentationSimplifiee = Column(Boolean, nullable = True)
  DepFoncN2 = Column(Float, nullable= True)
  RecFoncN2 = Column(Float, nullable= True)
  DepInvN2 = Column(Float, nullable= True)
  RecInvN2 = Column(Float, nullable= True)
  CodTypBud = Column(String)
  CodBud = Column(String)
  ProjetBudget = Column(Boolean, nullable = True)
  Affect = Column(String, nullable = True)
  SpecifBudget = Column(String, nullable = True)
  FinJur = Column(String, nullable = True)
  Md5 = Column(String, nullable = True)
  Sha1 = Column(String, nullable = True)

 class bloc_budget(Base) : 
  __tablename__ = 'bloc_budget'
  id = Column(Integer, primary_key= True, autoincrement= True)
  Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier'))

  Nomenclature = Column(String)
  Exer = Column(Integer)
  TypOpBudg = Column(String, nullable= True)
  Operation = Column(String, nullable= True)
  Nature = Column(String, nullable= True)
  type_nature = Column(String, nullable= True)
  Lib_court_nat_compte = Column(String, nullable = True)
  Libelle_nat_compte = Column(String, nullable = True)
  Lib_court_nat_chap = Column(String, nullable = True)
  Libelle_nat_chap = Column(String, nullable = True)
  ContNat = Column(String, nullable= True)
  LibCpte = Column(String, nullable= True)
  Fonction = Column(String, nullable= True)
  type_fonction = Column(String, nullable= True)
  Lib_court_fonc_compte = Column(String, nullable = True)
  Libelle_fonc_compte = Column(String, nullable = True)
  Lib_court_fonc_chap = Column(String, nullable = True)
  Libelle_fonc_chap = Column(String, nullable = True)
  ContFon = Column(String, nullable= True)
  ArtSpe = Column(Boolean, nullable= True)
  CodRD = Column(String)
  MtBudgPrec = Column(Float, nullable= True)
  MtRARPrec = Column(Float, nullable= True)
  MtPropNouv = Column(Float, nullable= True)
  MtPrev = Column(Float, nullable= True)
  OpBudg = Column(String)
  CredOuv = Column(Float, nullable= True)
  MtReal = Column(Float, nullable= True)
  MtRAR3112 = Column(Float, nullable= True)
  ContOp = Column(String, nullable= True)
  OpeCpteTiers = Column(String, nullable= True)
  MtSup = Column(String, nullable= True) 
  MtSup_APVote = Column(Float, nullable= True)     #temp
  MtSup_MtAPVote = Column(Float, nullable= True)   #temp 
  MtSup_Brut = Column(Float, nullable= True)
  MtSup_BudgetHorsRAR = Column(Float, nullable= True)
  MtSup_Comp = Column(String, nullable= True)
  MtSup_ICNE = Column(Float, nullable= True)
  MtSup_ICNEPrec = Column(Float, nullable= True)
  MtSup_MtOpeCumul = Column(Float, nullable= True)
  MtSup_MtOpeInfo = Column(Float, nullable= True)
  MtSup_Net = Column(Float, nullable= True)
  MtSup_MtPropNouv = Column(Float, nullable= True)
  MtSup_ProdChaRat = Column(String, nullable= True)
  MtSup_RARPrec = Column(String, nullable= True)

  CaracSup = Column(String, nullable= True)
  CaracSup_TypOpe = Column(Integer, nullable= True)
  CaracSup_Section = Column(String, nullable= True)
  CaracSup_ChapSpe = Column(String, nullable= True)
  CaracSup_ProgAutoLib = Column(String, nullable= True)
  CaracSup_ProgAutoNum = Column(String, nullable= True)
  CaracSup_VirCredNum = Column(String, nullable= True)
  CaracSup_CodeRegion = Column(Integer, nullable= True)

  DEquip_nat_compte = Column(String, nullable= True)
  DOES_nat_compte = Column(String, nullable = True)
  DOIS_nat_compte = Column(String, nullable = True)
  DR_nat_compte = Column(String, nullable = True)
  REquip_nat_compte = Column(String, nullable = True)
  ROES_nat_compte = Column(String, nullable = True)
  ROIS_nat_compte = Column(String, nullable = True)
  RR_nat_compte = Column(String, nullable = True)
  RegrTotalise_nat_compte = Column(String, nullable = True)
  Supprime_nat_compte = Column(String, nullable = True)
  SupprimeDepuis_nat_compte = Column(String, nullable = True)
  PourEtatSeul_nat_compte = Column(String, nullable = True)
  PourEtatSeul_nat_chap = Column(String, nullable = True)
  Section_nat_chap = Column(String, nullable = True)
  Special_nat_chap = Column(String, nullable = True)
  TypeChapitre_nat_chap = Column(String, nullable = True)
  DEquip_fonc_compte = Column(String, nullable = True)
  DOES_fonc_compte = Column(String, nullable = True)
  DOIS_fonc_compte = Column(String, nullable = True)
  DR_fonc_compte = Column(String, nullable = True)
  REquip_fonc_compte = Column(String, nullable = True)
  ROES_fonc_compte = Column(String, nullable = True)
  ROIS_fonc_compte = Column(String, nullable = True)
  RR_fonc_compte = Column(String, nullable = True)
  RegrTotalise_fonc_compte = Column(String, nullable = True)
  Supprime_fonc_compte = Column(String, nullable = True)
  SupprimeDepuis_fonc_compte = Column(String, nullable = True)
  Section_fonc_chap = Column(String, nullable = True)
  Special_fonc_chap = Column(String, nullable = True)
  TypeChapitre_fonc_chap = Column(String, nullable = True)

  document_budgetaire = relationship("document_budgetaire")

 class data_emprunt(Base) : 
    __tablename__ = 'data_emprunt'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier'))

    Nomenclature = Column(String)
    Exer = Column(Integer)
    
    CodTypEmpr = Column(String, nullable = True)
    CodProfilAmort = Column(String, nullable = True)
    CodProfilAmortDtVote = Column(String, nullable = True)
    ProfilAmort = Column(String, nullable = True) 
    ProfilAmortDtVote = Column(String, nullable = True) 
    CodArticle = Column(String, nullable = True) 
    LibCpte = Column(String, nullable = True) 
    AnEncaisse = Column(Integer, nullable = True) 
    ObjEmpr = Column(String, nullable = True) 
    MtEmprOrig = Column(Float, nullable = True)
    DureeRest = Column(Integer, nullable = True) 
    DureeRestInit = Column(Integer, nullable = True) 
    DureeRestReneg = Column(Integer, nullable = True) 
    CodTypPreteur = Column(String, nullable = True) 
    LibOrgaPreteur = Column(String, nullable = True)
    CodPeriodRemb = Column(String, nullable = True)
    CodPeriodRembDtVote = Column(String, nullable = True)
    CodPeriodRembReneg = Column(String, nullable = True)
    CodTyptxInit = Column(String, nullable = True)
    CodTyptxDtVote = Column(String, nullable = True)
    IndexTxVariInit = Column(String, nullable = True) 
    TxActuaInit = Column(Float, nullable = True)
    TxMargeInit = Column(Float, nullable = True)
    IndexTxVariDtVote = Column(String, nullable = True) 
    TxActua = Column(Float, nullable = True)
    IndiceEmpr = Column(String, nullable = True) 
    IndiceEmprDtVote = Column(String, nullable = True)
    MtIntExer = Column(Float, nullable = True) 
    MtCapitalExer = Column(Float, nullable = True)
    MtCapitalRestDu_01_01 = Column(Float, nullable = True) 
    MtICNE = Column(Float, nullable = True) 
    MtCapitalRestDu_31_12 = Column(Float, nullable = True)
    NomBenefEmprGaranti = Column(String, nullable = True)
    CodTypEmprGaranti = Column(String, nullable = True)
    CodNatEmpr = Column(String, nullable = True)
    DureeAnn = Column(Float, nullable = True) 
    TotGarEchoirExer = Column(Float, nullable = True) 
    AnnuitNetDette = Column(Float, nullable = True)
    ProvGarantiEmpr = Column(Float, nullable = True)
    RReelFon = Column(Float, nullable = True) 
    NumContrat = Column(String, nullable = True)
    PartGarantie = Column(Float, nullable = True)
    Tot1Annuite = Column(Float, nullable = True)
    IndSousJacent = Column(String, nullable = True)
    IndSousJacentDtVote = Column(String, nullable = True)
    Structure = Column(String, nullable = True)
    StructureDtVote = Column(String, nullable = True)

    DtSignInit = Column(DateTime, nullable = True) 
    DtEmission = Column(DateTime, nullable = True) 
    Dt1RembInit = Column(DateTime, nullable = True) 

    Txinit = Column(Float, nullable = True) 
    RtAnticipe = Column(Boolean, nullable = True)
    CoutSortie = Column(String, nullable = True)
    TypeSortie = Column(String, nullable = True)
    MtSortie = Column(Float, nullable = True)
    Couverture = Column(Boolean, nullable = True)
    MtCouvert = Column(Float, nullable = True) 
    MtCRDCouvert = Column(Float, nullable = True) 
    Renegocie = Column(Boolean, nullable = True)
    DureeContratInit = Column(Float, nullable = True) 
    DtPeriodeBonif = Column(String, nullable = True)
    TxMini = Column(String, nullable = True)
    TxMaxi = Column(String, nullable = True)
    TxApresCouv = Column(String, nullable = True)
    MtInt778 = Column(Float, nullable = True) 

    DtFinContr = Column(DateTime, nullable = True) 

    LibOrgCoContr = Column(String, nullable = True) 
    TypCouv = Column(String, nullable = True) 
    NatCouv = Column(String, nullable = True) 
    MtCouv = Column(Float, nullable = True)

    DtDebCouv = Column(DateTime, nullable = True) 
    DtFinCouv = Column(DateTime, nullable = True)

    CodTypTxCouv = Column(String, nullable = True)
    IndiceCouv = Column(String, nullable = True) 
    DtRegltCouv = Column(String, nullable = True) 
    MtCommCouv = Column(Float, nullable = True) 
    MtPrimePayeeCouv = Column(Float, nullable = True) 
    MPrimeRecueCouv = Column(Float, nullable = True) 
    TxPaye = Column(Float, nullable = True) 
    TxRecu = Column(Float, nullable = True) 
    MtCharges = Column(Float, nullable = True) 
    MtProduits = Column(Float, nullable = True) 
    IndSousJacentAvantCouv = Column(String, nullable = True)
    StuctureAvantCouv = Column(String, nullable = True)
    IndSousJacentApresCouv = Column(String, nullable = True)
    StuctureApresCouv = Column(String, nullable = True)
    DtReneg = Column(DateTime, nullable = True) 
    DureeContratReneg = Column(Integer, nullable = True) 
    CodTypTxReneg = Column(String, nullable = True)
    IndexTxVariReneg = Column(String, nullable = True) 
    TxActuaReneg = Column(Float, nullable = True) 
    CodProfilAmortReneg = Column(String, nullable = True)
    MtEmprReneg = Column(Float, nullable = True) 
    MtCRDRefin = Column(Float, nullable = True) 
    MtCapitalReamenage = Column(Float, nullable = True) 
    Champ_Editeur = Column(String, nullable = True) 

    document_budgetaire = relationship("document_budgetaire")

 class data_charge(Base) :
    __tablename__ = 'data_charge'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier'))
    Nomenclature = Column(String)
    Exer = Column(Integer)

    CodTypeCharge = Column(String, nullable=False)
    Exer_annexe  = Column(Integer, nullable = True)
    NatDepTransf = Column(String)
    DureeEtal  = Column(Integer, nullable = True)
    DtDelib = Column(DateTime, nullable=False)
    MtDepTransf  = Column(Float, nullable = True)
    MtAmort  = Column(Float, nullable = True)
    MtDotAmort  = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable= True)

    document_budgetaire = relationship("document_budgetaire")

 class data_concours(Base) : 
    __tablename__ = 'data_concours'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier'))
    Nomenclature = Column(String)
    Exer = Column(Integer)

    CodArticle = Column(String, nullable= True)
    CodInvFonc = Column(String, nullable= True)
    CodNatJurBenefCA = Column(String, nullable= True)
    LibOrgaBenef = Column(String)
    MtSubv = Column(Float)
    LibPrestaNat = Column(String, nullable= True)
    DenomOuNumSubv = Column(String, nullable= True)
    ObjSubv = Column(String, nullable= True)
    Siret = Column(String, nullable= True)
    PopCommune = Column(Integer, nullable= True)
    Champ_Editeur = Column(String, nullable= True)

    document_budgetaire = relationship("document_budgetaire")

 class data_tresorerie(Base) : 
    __tablename__ = 'data_tresorerie'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer)

    CodArticle = Column(String)
    LibOrgaPret = Column(String)
    DtDec = Column(DateTime)
    MtMaxAutori = Column(Float)
    MtTirage = Column(Float)
    MtRemb = Column(Float)
    MtRestDu = Column(Float, nullable = True)
    IntManda = Column(Float, nullable = True)
    NumContrat = Column(String, nullable= True)
    MtRembInt = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable= True)

    document_budgetaire = relationship("document_budgetaire")

 class data_tiers(Base) : 
    __tablename__ = 'data_tiers'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer)

    CodOper = Column(Integer, nullable = True)
    LibOper = Column(String)
    DtDelib = Column(DateTime, nullable = True)
    CodRD = Column(String)
    CodChapitre = Column(String)
    CodOperR = Column(String, nullable= True)
    NatTrav = Column(String, nullable= True)
    TypOpDep = Column(String, nullable= True)
    MtRealCumulPrec = Column(Float, nullable = True)
    MtCredOuv = Column(Float)
    MtRealExer = Column(Float)
    RAR = Column(Float, nullable = True)
    MtCumulReal = Column(Float)
    Champ_Editeur = Column(String, nullable= True)


    document_budgetaire = relationship("document_budgetaire")

 class data_credit_bail(Base) : 
    __tablename__ = 'data_credit_bail'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer)

    ExerContr = Column(Integer)
    CodTypContr = Column(String)
    NatBienContr = Column(String)
    MtRedevExer = Column(Float)
    LibCredBail = Column(String)
    DureeContr = Column(Integer)
    MtRedevN_1 = Column(Float, nullable = True)
    MtRedevN_2 = Column(Float, nullable = True)
    MtRedevN_3 = Column(Float, nullable = True)
    MtRedevN_4 = Column(Float, nullable = True)
    MtRedevN_5 = Column(Float, nullable = True)
    MtCumulRest = Column(Float, nullable = True)
    NumContr = Column(Integer, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_ppp(Base) : 
    __tablename__ = 'data_ppp'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer)

    LibContr = Column(String) 
    AnnSignContr = Column(Integer) 
    NomOrgaContr = Column(String) 
    NatPrestaContr = Column(String, nullable = True) 
    MtTotContr = Column(Float) 
    MtRemunCoContr = Column(Float) 
    DureeContr = Column(Integer) 
    DtFinContr = Column(DateTime)
    PartInvest = Column(Float, nullable = True) 
    PartNetteInvest = Column(Float, nullable = True) 
    Champ_Editeur = Column(Float, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_recette_affectee(Base) :  
    __tablename__ = 'data_recette_affectee'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodRAffect = Column(String) 
    LibRAffect = Column(String) 
    CodChapitre = Column(String, nullable = True) 
    CodArticle = Column(String) 
    LibArticle = Column(String) 
    MtRAE0101 = Column(Float) 
    MtR = Column(Float) 
    MtD = Column(Float) 
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_fiscalite(Base) :   
    __tablename__ = 'data_fiscalite'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypContrib = Column(String) 
    CodSousTypContrib = Column(String)
    CodTypeCarburant = Column(String, nullable = True)
    LibTaxe = Column(String, nullable = True)
    MtBaseNotif = Column(Float, nullable = True)
    TxVariBase = Column(Float, nullable = True)
    TxApplicConsMunic = Column(Float, nullable = True)
    TxVariTx = Column(Float, nullable = True)
    MtProdVote = Column(Float, nullable = True)
    TxVariProd = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable = True)
    Origine = Column(String, nullable = True)
    Unite = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_autre_engagement(Base) :    
    __tablename__ = 'data_autre_engagement'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypAutEng = Column(String) 
    CodArticle = Column(String)
    CodTypPersoMorale = Column(String, nullable = True)
    AnnOrig = Column(Integer)
    NatEng = Column(String)
    NomOrgaBenef = Column(String)
    DureeEng = Column(Integer)
    CodePeriod = Column(String)
    MtDetteOrig = Column(Float)
    MtDette = Column(Float)
    MtAnnuit = Column(Float)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_formation(Base) :     
    __tablename__ = 'data_formation'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier"))
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    NomElu = Column(String)
    ActionFinanc = Column(String)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_consolidation(Base) :     
    __tablename__ = 'data_consolidation'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypBudAgreg = Column(String)
    CodBudAnnex = Column(String, nullable = True)
    LibBudAnnex = Column(String, nullable = True)
    CodInvFonc = Column(String)
    CodRD = Column(String)
    MtCredOuv = Column(Float)
    MtRealMandatTitre = Column(Float)
    RAR = Column(Float)
    SiretBudAnnexe = Column(String, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_organisme_eng(Base) :     
    __tablename__ = 'data_organisme_eng'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodNatEng = Column(String)
    DtEng = Column(DateTime, nullable = True)
    NatEng = Column(String, nullable = True)
    NomOrgEng = Column(String)
    RSOrgEng = Column(String, nullable = True)
    NatJurOrgEng = Column(String, nullable = True)
    MtOrgEng = Column(Float)
    Champ_Editeur = Column(String, nullable = True)
    
    document_budgetaire = relationship("document_budgetaire")

 class data_organisme_group(Base) :      
    __tablename__ = 'data_organisme_group'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodNatOrgGroup = Column(String)
    NomOrgGroup = Column(String)
    DtAdhGroup = Column(DateTime, nullable = True)
    CodModFinanc = Column(String, nullable = True)
    MtFinancOrgGroup  = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable = True)
    
    document_budgetaire = relationship("document_budgetaire")

 class data_patrimoine(Base) :      
    __tablename__ = 'data_patrimoine'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodVariPatrim = Column(String) 
    CodEntreeSorti = Column(String, nullable = True)
    CodModalAcqui = Column(String, nullable = True)
    CodModalSorti = Column(String, nullable = True)
    CodTypImmo = Column(String, nullable = True)
    LibBien = Column(String) 
    MtValAcquiBien = Column(Float) 
    MtCumulAmortBien = Column(Float, nullable = True)
    MtAmortExer= Column(Float, nullable = True)
    DureeAmortBien = Column(Integer, nullable = True)
    NumInventaire = Column(String, nullable = True)
    DtAcquiBien = Column(DateTime, nullable = True)
    MtVNCBien0101 = Column(Float, nullable = True)
    MtVNCBien3112 = Column(Float, nullable = True)
    MtVNCBienSorti = Column(Float, nullable = True)
    MtPrixCessBienSorti = Column(Float, nullable = True)
    DtCessBienSorti= Column(DateTime, nullable = True)
    CodTypTitre = Column(String, nullable = True)
    LibOrgPrisePartic = Column(String, nullable = True)
    DtDelib = Column(DateTime, nullable = True)
    LibObserv = Column(String, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_personnel(Base) :      
    __tablename__ = 'data_personnel'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypAgent = Column(String, nullable = True)
    EmploiGradeAgent = Column(String, nullable = True)
    CodCatAgent = Column(String, nullable = True)
    TempsComplet = Column(Boolean, nullable = True)
    Permanent = Column(Boolean, nullable = True)
    NatureContrat = Column(String, nullable = True)
    LibelleNatureContrat = Column(String, nullable = True)
    CodSectAgentTitulaire = Column(String, nullable = True)
    CodSectAgentNonTitulaire = Column(String, nullable = True)
    RemunAgent = Column(Float, nullable = True)
    MtPrev6215= Column(Float, nullable = True)
    IndiceAgent = Column(String, nullable = True)
    CodMotifContrAgent = Column(String, nullable = True)
    LibMotifContrAgent = Column(String, nullable = True)
    EffectifBud = Column(Float, nullable = True)
    EffectifPourvu = Column(Float, nullable = True)
    EffectifTNC = Column(Integer, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_personnel_solde(Base) :      
    __tablename__ = 'data_personnel_solde'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    NbrCreatEmploi = Column(Float, nullable = True)
    NbrSupprEmploi = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")
   
 class data_dette(Base) :      
    __tablename__ = 'data_dette'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    LibTypDette = Column(String) 
    MtInitDette = Column(Float) 
    MtDExerDette  = Column(Float) 
    MtRestDette  = Column(Float)
    Champ_Editeur = Column(String, nullable = True)
  
    document_budgetaire = relationship("document_budgetaire")
       
 class data_ventilation(Base) :      
    __tablename__ = 'data_ventilation'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypVentil = Column(String)
    NomService = Column(String, nullable = True)
    CodInvFonc = Column(String)
    CodRD = Column(String)
    TypOpBudg = Column(String)
    CodRegroup = Column(String, nullable = True)
    CodChapitre = Column(String, nullable = True)
    CodArticle = Column(String)
    LibCpte = Column(String)
    MtVentil = Column(Float)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_contrat_couv(Base) :      
    __tablename__ = 'data_contrat_couv'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypTx = Column(String, nullable = True)
    NumContratCouv = Column(String, nullable = True)
    CodTypRisqFinanc = Column(String, nullable = True)
    NbEmpruntCouv= Column(Integer, nullable = True)
    LibEmprCouv = Column(String, nullable = True)
    MtEmprCouv = Column(Float)
    CapitalRestDu = Column(Float, nullable = True)
    DtFinContrEmpr = Column(DateTime, nullable = True)
    TypCouv = Column(String, nullable = True)
    NatContrCouv = Column(String)
    LibOrgCoContr = Column(String)
    DtDebContr = Column(DateTime)
    DtFinCouv = Column(DateTime, nullable = True)
    CodPeriodRemb = Column(String, nullable = True)
    MtCommDiv = Column(Float, nullable = True)
    MtPrimePayee = Column(Float, nullable = True)
    MtPrimeRecue = Column(Float, nullable = True)
    IndexTxPaye = Column(String, nullable = True)
    TxTxPaye = Column(Float, nullable = True)
    IndexTxRecu = Column(String, nullable = True)
    TxTxRecu = Column(Float, nullable = True)
    StuctureAvantCouv = Column(String, nullable = True)
    StuctureApresCouv = Column(String, nullable = True)
    IndSousJacentAvantCouv = Column(String, nullable = True)
    IndSousJacentApresCouv = Column(String, nullable = True)
    MtChaOrig = Column(Float, nullable = True)
    MtProdOrig = Column(Float, nullable = True)
    DureeContr = Column(Integer, nullable = True)
    DtReglt = Column(DateTime, nullable = True)
    MtMaxAutori_N = Column(Float, nullable = True)
    MtMaxAutoriEmprEnc_N = Column(Float, nullable = True)
    MtChaOrigPrimeAss = Column(Float, nullable = True)
    MtChaOrigPrimeCommi = Column(Float, nullable = True)
    MtPert = Column(Float, nullable = True)
    MtProf = Column(Float, nullable = True)
    MtPertProf = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_amortissement_methode(Base) :      
    __tablename__ = 'data_amortissement_methode'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    LibBienAmort = Column(String)
    DureeBienAmort = Column(Integer)
    DtDelib = Column(DateTime, nullable = True)
    ProcAmort = Column(String, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_provision(Base) :      
    __tablename__ = 'data_provision'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypTabProv = Column(String)
    CodTypProv = Column(String, nullable = True)
    CodSTypProv = Column(String)
    LibNatProv = Column(String, nullable = True)
    MtProvExer = Column(Float, nullable = True)
    DtConstitProv = Column(DateTime, nullable = True)
    MtProvConstit_01_01_N = Column(Float, nullable = True)
    MtProvRepr = Column(Float)
    LibObjProv = Column(String)
    MtTotalProvAConstit = Column(Float, nullable = True)
    DureeEtal = Column(Integer, nullable = True)
    CodNatProv = Column(String, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_apcp(Base) :       
    __tablename__ = 'data_apcp'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypAutori = Column(String) 
    CodSTypAutori = Column(String) 
    NumAutori = Column(String, nullable = True) 
    LibAutori = Column(String, nullable = True) 
    Chapitre = Column(String, nullable = True) 
    MtAutoriPrec = Column(Float, nullable = True) 
    MtAutori_NMoins1 = Column(Float, nullable = True) 
    MtAutoriPropose = Column(Float, nullable = True) 
    MtAutoriVote = Column(Float, nullable = True) 
    MtAutoriDispoAffectation = Column(Float, nullable = True) 
    MtAutoriNonCouvParCP_01_01_N = Column(Float, nullable = True) 
    MtAutoriAffectee = Column(Float, nullable = True) 
    MtAutoriAffecteeAnnulee = Column(Float, nullable = True) 
    MtCPAnt = Column(Float, nullable = True) 
    MtCPOuv = Column(Float, nullable = True) 
    MtCPReal = Column(Float, nullable = True) 
    MtCredAFinanc_NPlus1 = Column(Float, nullable = True) 
    MtCredAFinanc_Sup_N = Column(Float, nullable = True) 
    MtCredAFinanc_Sup_NPlus1 = Column(Float, nullable = True) 
    RatioCouvAutoriAffect_N = Column(Float, nullable = True) 
    RatioCouvAutoriAffect_NMoins1 = Column(Float, nullable = True) 
    RatioCouvAutoriAffect_NMoins2 = Column(Float, nullable = True) 
    RatioCouvAutoriAffect_NMoins3= Column(Float, nullable = True) 
    Champ_Editeur = Column(String, nullable = True) 
    TypeChapitre = Column(String, nullable = True) 

    document_budgetaire = relationship("document_budgetaire")

 class data_signature(Base) :       
    __tablename__ = 'data_signature'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    NbrMembExer = Column(Integer)
    NbrMembPresent = Column(Integer)
    NbrSuffExprime = Column(Integer)
    NbrVotePour = Column(Integer)
    NbrVoteContre = Column(Integer)
    NbrVoteAbstention = Column(Integer)
    DtConvoc = Column(DateTime)
    LibPresentPar = Column(String, nullable = True)
    LibPresentLieu = Column(String)
    DtPresent = Column(DateTime)
    LibDelibPar = Column(String)
    LibReuniSession = Column(String, nullable = True)
    LibDelibLieu = Column(String)
    DtDelib = Column(DateTime)
    DtTransmPrefect = Column(DateTime, nullable = True)
    DtPub = Column(DateTime, nullable = True)
    LibFin = Column(String, nullable = True)
    DtfFin = Column(DateTime, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_signataire(Base) :       
    __tablename__ = 'data_signataire'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    Signataire = Column(String)

    document_budgetaire = relationship("document_budgetaire")

 class data_etab_service(Base) :       
    __tablename__ = 'data_etab_service'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodNatEtab = Column(String) 
    LibCatEtab = Column(String, nullable = True)
    LibEtab = Column(String) 
    SiretEtab = Column(String, nullable = True)
    DtCreatEtab = Column(DateTime, nullable = True)
    NumDelibEtab = Column(String, nullable = True)
    DtDelibEtab = Column(DateTime, nullable = True)
    LibNatActivEtab = Column(String, nullable = True)
    IndicTVAEtab = Column(Boolean, nullable = True)
    Champ_Editeur = Column(String, nullable = True)
     
    document_budgetaire = relationship("document_budgetaire")

 class data_pret(Base) :       
    __tablename__ = 'data_pret'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypPret = Column(String) 
    NomBenefPret  = Column(String) 
    DtDelib = Column(DateTime, nullable = True)
    MtCapitalRestDu_01_01 = Column(Float, nullable = True)
    MtCapitalRestDu_31_12 = Column(Float, nullable = True)
    MtCapitalExer = Column(Float, nullable = True)
    MtIntExer = Column(Float, nullable = True)
    MtICNE = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_contrat_couv_reference(Base) :       
    __tablename__ = 'data_contrat_couv_reference'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    NumContr = Column(String)
    NumContratEmprunt = Column(String, nullable = True)
    MtEmprOrig = Column(Float, nullable = True)
    DureeAnn = Column(Integer, nullable = True)
    CodTyptxInit = Column(String, nullable = True)
    TxActuaInit = Column(Float, nullable = True)
    IndexTxVariInit = Column(String, nullable = True)
    CodProfilAmort = Column(String, nullable = True)
    DtDebEcheance = Column(DateTime, nullable = True)
    LibObserv = Column(String, nullable = True)
    MtCapitalRestDu_01_01 = Column(Float, nullable = True)
    MtCapitalRestDu_31_12 = Column(Float, nullable = True)
    MtCapitalExer = Column(Float, nullable = True)
    MtIntExer = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_service_ferroviaire_bud(Base) :       
    __tablename__ = 'data_service_ferroviaire_bud'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    
    Exer = Column(Integer) 
    
    CodRD = Column(String)
    CodInvFonc = Column(String)
    CodRegroupBudFerrov = Column(String)
    CodChapitre = Column(String)
    MtVentil = Column(Float)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_service_ferroviaire_patrim(Base) :       
    __tablename__ = 'data_service_ferroviaire_patrim'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey('document_budgetaire.Id_Fichier')) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 
    
    LibRame = Column(String)
    Matricule = Column(String)
    DtMiseService = Column(DateTime, nullable = True)
    DtFinPot = Column(DateTime)
    LibProprietaire = Column(String)
    LibModeFinanc = Column(String, nullable = True)
    MtValOrig = Column(Float)
    MtAmort = Column(Float, nullable = True)
    MtVNC = Column(Float, nullable = True)
    Champ_Editeur = Column(String, nullable = True)  

    document_budgetaire = relationship("document_budgetaire")

 class data_service_ferroviaire_ter(Base) :       
    __tablename__ = 'data_service_ferroviaire_ter'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 
    
    CodCptTER = Column(String)
    MtCptTER = Column(Float)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_fond_comm_hebergement(Base) :       
    __tablename__ = 'data_fond_comm_hebergement'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 
    
    CodOper = Column(Integer)
    LibFondHeberg = Column(String)
    CodRD = Column(String)
    LibEtabHeberg = Column(String)
    MtFond = Column(Float)
    LibObjFond = Column(String, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_fond_europeen(Base) :       
    __tablename__ = 'data_fond_europeen'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 
    
    LibFondsEuropeen = Column(String) 
    CodDestFonds = Column(String) 
    CodRDDJust = Column(String) 
    LibMesure = Column(String) 
    CodArticle = Column(String) 
    MtFond = Column(Float) 
    LibBenef = Column(String, nullable = True)
    LibOper = Column(String, nullable = True)
    LibEmetteurs = Column(String, nullable = True)
    DtAcquit = Column(DateTime, nullable = True)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_fond_europeen_programmation(Base) :       
    __tablename__ = 'data_fond_europeen_programmation'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 
    
    Programmation = Column(String)
    TypeGestion = Column(String) 
    TypeFonds = Column(String) 
    CodRD = Column(String, nullable = True)
    RappelTotal = Column(Float, nullable = True)
    MontantN_X = Column(Float, nullable = True)
    MontantN = Column(Float, nullable = True)
    RegulN = Column(Float, nullable = True)
    Avances = Column(Float, nullable = True) 

    document_budgetaire = relationship("document_budgetaire")

 class data_fond_aides_eco(Base) :        
    __tablename__ = 'data_fond_aides_eco'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 
    
    LibOrgConvent = Column(String, nullable = True)
    DtConvent = Column(DateTime)
    CodRD = Column(String)
    CodInvFon = Column(String)
    DtVers = Column(DateTime)
    MtReliquatCPAnt = Column(Float)
    MtVersExer = Column(Float)
    CodArticle = Column(String)
    MtTotAide = Column(Float)
    LibBenef = Column(String, nullable = True)
    LibAide = Column(String, nullable = True)
    LibFormeAide = Column(String, nullable = True)
    MtDExerAnt = Column(Float)
    MtDExer = Column(Float)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_formation_pro_jeunes(Base) :        
    __tablename__ = 'data_formation_pro_jeunes'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodRDTot = Column(String)
    CodRessExt = Column(String, nullable = True)
    CodApprent = Column(String)
    MtFormN = Column(Float)
    MtFormN_1 = Column(Float)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class data_membresasa(Base) :        
    __tablename__ = 'data_membresasa'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    Commune = Column(String) 
    Proprietaire = Column(String) 
    Superficie = Column(Float)

    document_budgetaire = relationship("document_budgetaire")

 class data_flux_croises(Base) :        
    __tablename__ = 'data_flux_croises'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodTypFlux = Column(String) 
    CodInvFonc = Column(String) 
    CodRD = Column(String) 
    MtCredOuv = Column(Float) 
    MtReal = Column(Float) 
    MtRAR = Column(Float) 

    document_budgetaire = relationship("document_budgetaire")

 class data_sommaire(Base) :        
    __tablename__ = 'data_sommaire'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    Nomenclature = Column(String)
    Exer = Column(Integer) 

    CodeAnnexe = Column(String)
    Present = Column(Boolean)
    Champ_Editeur = Column(String, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class fonction_compte(Base) : 
    __tablename__ = 'fonction_compte'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Exer = Column(Integer) 
    Nomenclature= Column(String, nullable = True)
    Code_fonc_compte= Column(String, nullable = True)
    DEquip_fonc_compte= Column(String, nullable = True)
    DOES_fonc_compte= Column(String, nullable = True)
    DOIS_fonc_compte= Column(String, nullable = True)
    DR_fonc_compte= Column(String, nullable = True)
    Lib_court_fonc_compte= Column(String, nullable = True)
    Libelle_fonc_compte= Column(String, nullable = True)
    REquip_fonc_compte= Column(String, nullable = True)
    ROES_fonc_compte= Column(String, nullable = True)
    ROIS_fonc_compte= Column(String, nullable = True)
    RR_fonc_compte= Column(String, nullable = True)
    RegrTotalise_fonc_compte= Column(String, nullable = True)
    Supprime_fonc_compte= Column(String, nullable = True)
    SupprimeDepuis_fonc_compte = Column(String, nullable = True)

 class fonction_chapitre(Base) : 
    __tablename__ = 'fonction_chapitre'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Exer = Column(Integer) 
    Nomenclature = Column(String)
    Code_fonc_chap = Column(String, nullable = True)
    Lib_court_fonc_chap = Column(String, nullable = True)
    Libelle_fonc_chap = Column(String, nullable = True)
    Section_fonc_chap = Column(String, nullable = True)
    Special_fonc_chap = Column(String, nullable = True)
    TypeChapitre_fonc_chap = Column(String, nullable = True)

 class fonction_referentielle(Base) : 
    __tablename__ = 'fonction_referentielle'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Exer = Column(Integer) 
    Nomenclature = Column(String)
    Code_fonc_ref = Column(String, nullable = True)
    Lib_court_fonc_ref = Column(String, nullable = True)
    Libelle_fonc_ref = Column(String, nullable = True)

 class nature_compte(Base) : 
    __tablename__ = 'nature_compte'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Code_nat_compte = Column(String, nullable = True)
    DEquip_nat_compte = Column(String, nullable = True)
    DOES_nat_compte = Column(String, nullable = True)
    DOIS_nat_compte = Column(String, nullable = True)
    DR_nat_compte = Column(String, nullable = True)
    Lib_court_nat_compte = Column(String, nullable = True)
    Libelle_nat_compte = Column(String, nullable = True)
    REquip_nat_compte = Column(String, nullable = True)
    ROES_nat_compte = Column(String, nullable = True)
    ROIS_nat_compte = Column(String, nullable = True)
    RR_nat_compte = Column(String, nullable = True)
    RegrTotalise_nat_compte = Column(String, nullable = True)
    Supprime_nat_compte = Column(String, nullable = True)
    SupprimeDepuis_nat_compte = Column(String, nullable = True)
    Exer = Column(Integer) 
    Nomenclature = Column(String)
    PourEtatSeul_nat_compte = Column(String, nullable = True)

 class nature_chapitre(Base) : 
    __tablename__ = 'nature_chapitre'
    id = Column(Integer, primary_key= True, autoincrement= True)    
    Code_nat_chap = Column(String, nullable = True)
    Lib_court_nat_chap = Column(String, nullable = True)
    Libelle_nat_chap = Column(String, nullable = True)
    PourEtatSeul_nat_chap = Column(String, nullable = True)
    Section_nat_chap = Column(String, nullable = True)
    Special_nat_chap = Column(String, nullable = True)
    TypeChapitre_nat_chap = Column(String, nullable = True)
    Exer = Column(Integer) 
    Nomenclature = Column(String)

 class transcodage(Base) : 
    __tablename__ = 'transcodage' 
    id = Column(Integer, primary_key= True, autoincrement= True)
    nom_champ = Column(String, nullable = True)
    enum = Column(String, nullable = True)

 class anomalies(Base) : 
    __tablename__ = 'anomalies'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Id_Fichier = Column(String, ForeignKey("document_budgetaire.Id_Fichier")) 
    fonctions_sans_ref = Column(String, nullable = True)
    nb_fonctions = Column(Integer, nullable = True)
    pourcentage = Column(Float, nullable = True)

    document_budgetaire = relationship("document_budgetaire")

 class fonction_compte_mixte(Base) : 
    __tablename__ = 'fonction_compte_mixte'
    id = Column(Integer, primary_key= True, autoincrement= True)
    Exer = Column(Integer) 
    Nomenclature = Column(String)
    Code_fonc_compte = Column(String, nullable = True)
    Lib_court_fonc_compte = Column(String, nullable = True)
    Libelle_fonc_compte = Column(String, nullable = True)
    Code_mixte = Column(String, nullable = True)
    code_chap_mixte = Column(String, nullable = True)

 class info_siret(Base) : 
   __tablename__ = 'info_siret'
   id = Column(Integer, primary_key = True, autoincrement = True)
   CODGEO = Column(String, nullable = True)
   LIBGEO = Column(String, nullable = True)
   DEP = Column(String, nullable = True)
   REG = Column(String, nullable = True)
   EPCI = Column(String, nullable = True)
   NATURE_EPCI = Column(String, nullable = True)
   libelleCategorieJuridique = Column(String, nullable = True)  
   siren = Column(String, nullable = True)
   siret = Column(String, nullable = True)
   denominationUniteLegale = Column(String, nullable = True)

 Base.metadata.drop_all(engine)
 Base.metadata.create_all(engine)

def decoupage_code(code) : 
  if len(code) == 3:
    return code[1:]
  elif len(code) >= 4:
    return code[2:]
  else:
    return code

@task.branch(task_id = "injection_donnees_quali")
def parquet_to_bdd() : 

  engine = ouverture_connection_postgres(Variable.get("pg_conn_id"))

  #transfo des données 
  fco = pd.read_parquet('./data_test/ressources/concat/Fonction_Compte.parquet')
  fco = fco.add_suffix('_fonc_compte')
  fco = fco.rename(columns = {'Exer_fonc_compte' : 'Exer', 'Nomenclature_fonc_compte' : 'Nomenclature'})
  fco['Exer'] = fco['Exer'].astype(str)

  fch = pd.read_parquet('./data_test/ressources/concat/Fonction_Chapitre.parquet')
  fch = fch.add_suffix('_fonc_chap')
  fch = fch.rename(columns = {'Exer_fonc_chap' : 'Exer', 'Nomenclature_fonc_chap' : 'Nomenclature'})
  fch['Exer'] = fch['Exer'].astype(str)

  fref = pd.read_parquet('./data_test/ressources/concat/Fonction_Referentiel.parquet')
  fref = fref.add_suffix('_fonc_ref')
  fref = fref.rename(columns = {'Exer_fonc_ref' : 'Exer', 'Nomenclature_fonc_ref' : 'Nomenclature'})
  fref['Exer'] = fref['Exer'].astype(str)

  nco = pd.read_parquet('./data_test/ressources/concat/Nature_Compte.parquet')
  nco = nco.add_suffix('_nat_compte')
  nco = nco.rename(columns = {'Exer_nat_compte' : 'Exer', 'Nomenclature_nat_compte' : 'Nomenclature'})
  nco['Exer'] = nco['Exer'].astype(str)

  nch = pd.read_parquet('./data_test/ressources/concat/Nature_Chapitre.parquet')
  nch = nch.add_suffix('_nat_chap')
  nch = nch.rename(columns = {'Exer_nat_chap' : 'Exer', 'Nomenclature_nat_chap' : 'Nomenclature'})
  nch['Exer'] = nch['Exer'].astype(str)

  fco_mixte = fco.copy()
  fco_mixte['Code_mixte'] = fco_mixte['Code_fonc_compte'].apply(decoupage_code)
  fco_mixte = fco_mixte.drop_duplicates(subset=['Code_mixte', 'Nomenclature','Exer'], keep='first')
  fco_mixte['code_chap_mixte'] = fco_mixte['DOES_fonc_compte'] + fco_mixte['DOIS_fonc_compte'] + fco_mixte['DR_fonc_compte']
  fco_mixte = fco_mixte[['Exer','Nomenclature','Code_fonc_compte','Lib_court_fonc_compte','Libelle_fonc_compte','Code_mixte','code_chap_mixte']]
  fco_mixte = fco_mixte[['Exer','Nomenclature','Code_fonc_compte','Lib_court_fonc_compte','Libelle_fonc_compte','Code_mixte','code_chap_mixte']]

  dico_transco = pd.read_csv('./data_test/ressources/dictionnaire_v2.csv')
  dico_transco = dico_transco.drop(columns=['Unnamed: 0']) 

  info_siret = pd.read_parquet('./data_test/ressources/info_siren_coupe.parquet')


  #Envoi des données 
  fco.to_sql('fonction_compte',engine , if_exists = 'replace', index = False, method = 'multi')
  fch.to_sql('fonction_chapitre',engine , if_exists = 'replace', index = False, method = 'multi')
  fco_mixte.to_sql('fonction_compte_mixte',engine , if_exists = 'replace', index = False, method = 'multi')
  fref.to_sql('fonction_referentielle',engine , if_exists = 'replace', index = False, method = 'multi')
  nch.to_sql('nature_chapitre',engine , if_exists = 'replace', index = False, method = 'multi')
  nco.to_sql('nature_compte',engine , if_exists = 'replace', index = False, method = 'multi')
  dico_transco.to_sql('transcodage', engine, if_exists = 'replace', index = False, method = 'multi')
  info_siret.to_sql('info_siret',engine , if_exists = 'replace', index = False, method = 'multi')

with DAG(dag_id="setup_tables", start_date=datetime(2022, 4, 2)) as dag : 

  creation_tables()
  parquet_to_bdd()


 