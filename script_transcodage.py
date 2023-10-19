import requests
from lxml import etree
import pandas as pd 
import os
import sqlite3


DOSSIER_PARENT = "."
URL_MERE_PLAN_DE_COMPTE = "http://odm-budgetaire.org/composants/normes/"
PLAN_COMPTE = "planDeCompte.xml"

BDD = './bdd_actes_budgetaires.db'
DOSSIER_TRANSCODAGE = "./csv_transcodage/"
ANNEE = '2020'



#Scraping, sécurité anti doublon via connexion à la bdd

def _recherche_nomenclature_dans_bdd() -> list: 
 ''' Va chercher les nomenclatures distinctes dans la table voulue'''
 try : 
  conn = sqlite3.connect(BDD)
  cursor = conn.cursor()
  cursor.execute(''' SELECT DISTINCT Nomenclature FROM actes_budgetaire''')
  nomenclatures = cursor.fetchall()
  liste_nomenclature_brut = [row[0] for row in nomenclatures]
  nomenclature_done = cursor.execute(''' SELECT DISTINCT Nomenclature FROM Transcode_Nature''').fetchall()
  liste_doublon = [row[0] for row in nomenclature_done]
  liste_nomenclature = [objet for objet in liste_nomenclature_brut if objet not in liste_doublon]

 finally : #Tres embetant de devoir restart le notebook à chaque tentative ratée
  if conn :
   conn.close()
 return liste_nomenclature

def _recherche_plan_de_compte(annee, nomenclature) : 
 ''' Va chercher l'adresse http du xml de transcodage '''
 adresse_du_xml = URL_MERE_PLAN_DE_COMPTE + f"{annee}/" + f"{nomenclature.split('-')[0]}/" + f"{nomenclature.split('-')[1]}/"
 return adresse_du_xml

def scrap_plan_de_compte(annee_cible) -> dict:
 Liste_adresse = []
 Liste_nomenclature = _recherche_nomenclature_dans_bdd()
 for i in Liste_nomenclature :
  Liste_adresse.append(_recherche_plan_de_compte(annee_cible, i))
 Dico_nomenclatures = dict(zip(Liste_adresse, Liste_nomenclature))
 return Dico_nomenclatures

#Traitement xml par xml

def racine_transcodage(adresse) :
 ''' Créer la racine lxml du xml ''' 
 Requete = requests.get(adresse + PLAN_COMPTE)
 Racine = etree.fromstring(Requete.content)
 Enfants = Racine.getchildren()
 return Enfants

def extraction_nature(Enfants, nomenclature) -> pd.DataFrame: 
 ''' Permet de récupérer les lignes de la branche Nature ( Nature et ContNat )'''
 Nature_Chapitre = Enfants[0].getchildren()[0].xpath(".//*[@Code]")
 Nature_Compte = Enfants[0].getchildren()[1].xpath(".//*[@Code]")
 Liste_Nature_Chapitre = []
 Liste_Nature_Compte = []

 for i in Nature_Chapitre :
  Liste_Nature_Chapitre.append(i.attrib)
 df_Nature_Chapitre = pd.DataFrame(Liste_Nature_Chapitre)
 df_Nature_Chapitre['Nomenclature'] = nomenclature

 for i in Nature_Compte : 
  Liste_Nature_Compte.append(i.attrib)
 df_Nature_Compte = pd.DataFrame(Liste_Nature_Compte)
 df_Nature_Compte['Nomenclature'] = nomenclature

 return df_Nature_Chapitre, df_Nature_Compte

def extraction_fonction(Enfants, nomenclature) -> pd.DataFrame:
 ''' Permet de récupérer les lignes de la branche Fonction ( Fonction et Fonction Compte et Fonction ref, ret, machin )'''
 Fonction_chapitre = Enfants[1].getchildren()[0].xpath(".//*[@Code]")
 Fonction_compte = Enfants[1].getchildren()[1].xpath(".//*[@Code]")
 Fonction_ret = Enfants[1].getchildren()[2].xpath(".//*[@Code]")

 Liste_Fonction_Chapitre = []
 Liste_Fonction_Compte = []
 Liste_Fonction_Ret = []

 for i in Fonction_chapitre:
    Liste_Fonction_Chapitre.append(i.attrib)
 df_Fonction_Chapitre = pd.DataFrame(Liste_Fonction_Chapitre)
 df_Fonction_Chapitre['Nomenclature'] = nomenclature

 for i in Fonction_compte:
    Liste_Fonction_Compte.append(i.attrib)
 df_Fonction_Compte = pd.DataFrame(Liste_Fonction_Compte)
 df_Fonction_Compte['Nomenclature'] = nomenclature

 for i in Fonction_ret:
    Liste_Fonction_Ret.append(i.attrib)
 df_Fonction_Ret = pd.DataFrame(Liste_Fonction_Ret)
 df_Fonction_Ret['Nomenclature'] = nomenclature

 return df_Fonction_Chapitre, df_Fonction_Compte, df_Fonction_Ret

#Securite si ODM nous dit adieu

def _ajout_origine(df_Nat, df_ContNat, df_Fon, df_ContFon, df_FonRet) -> pd.DataFrame : 
 ''' Ajout de l'origine uniquement pour la création d'un csv commun'''
 df_Nat_Save = df_Nat.copy()
 df_Nat_Save['Origine'] = 'Nature'

 df_ContNat_Save = df_ContNat.copy()
 df_ContNat_Save['Origine'] = 'Nature_Compte'

 df_Fon_Save = df_Fon.copy()
 df_Fon_Save['Origine'] = 'Nature_Compte'

 df_ContFon_Save = df_ContFon.copy()
 df_ContFon_Save['Origine'] = 'Fonction Compte'
 
 df_FonRet_Save = df_FonRet.copy()
 df_FonRet_Save['Origine'] = 'Fonction RefFonctionnelles'
 return df_Nat_Save, df_ContNat_Save, df_Fon_Save, df_ContFon_Save, df_FonRet_Save
 
def creation_csv_commun(df_Nat, df_ContNat, df_Fon, df_ContFon, df_FonRet, nomenclature) :
 ''' Permet de créer une sauvegarde en brut au cas où'''
 df_Nat_2, df_ContNat_2, df_Fon_2, df_ContFon_2, df_FonRet_2 = _ajout_origine(
   df_Nat, df_ContNat, df_Fon, df_ContFon, df_FonRet)
 
 df_uni = pd.concat([df_Nat_2, df_ContNat_2, df_Fon_2, df_ContFon_2, df_FonRet_2], 
                    ignore_index= True)
 fichier_csv = os.path.join(DOSSIER_TRANSCODAGE, f"{nomenclature}.csv")
 df_uni.to_csv(fichier_csv, index = False)

#Insertion bdd

def insertion_dans_bdd(df_Nat, df_ContNat, df_Fon, df_ContFon, df_FonRet) : 
 """ insert dans une bdd les données maintenant transformées et en sort un csv à jour """
 chemin_bdd = os.path.join(DOSSIER_PARENT, BDD)
 conn = sqlite3.connect(chemin_bdd)
 df_Nat.to_sql('Transcode_Nature', conn,
                    if_exists='append', index=False)
 df_ContNat.to_sql('Transcode_Nature_Compte', conn,
                    if_exists='append', index=False) 
 df_Fon.to_sql('Transcode_Fonction', conn,
                    if_exists='append', index=False)
 df_ContFon.to_sql('Transcode_Fonction_Compte', conn,
                    if_exists='append', index=False)
 df_FonRet.to_sql('Transcode_Fonction_RefFonctionnelles', conn,
                    if_exists='append', index=False)
 conn.commit()
 conn.close()

 Liste_adresse = []
 Liste_nomenclature = _recherche_nomenclature_dans_bdd()
 for i in Liste_nomenclature :
  Liste_adresse.append(_recherche_plan_de_compte(annee_cible, i))
 Dico_nomenclatures = dict(zip(Liste_adresse, Liste_nomenclature))
 return Dico_nomenclatures

def scraping_transcodage_to_bdd_2020() : 
 dico_adresse_xml = scrap_plan_de_compte(ANNEE)
 for adresse, nomenclature in dico_adresse_xml.items() :
  enfant = racine_transcodage(adresse)
  df_nature, df_nature_compte = extraction_nature(enfant, nomenclature)
  df_fonction, df_fonction_compte, df_fonction_ref = extraction_fonction(enfant, nomenclature)
  creation_csv_commun(df_nature, df_nature_compte, df_fonction, df_fonction_compte, df_fonction_ref, nomenclature)
  insertion_dans_bdd(df_nature, df_nature_compte, df_fonction, df_fonction_compte, df_fonction_ref)


if __name__ == "__main__": 
 scraping_transcodage_to_bdd_2020()
