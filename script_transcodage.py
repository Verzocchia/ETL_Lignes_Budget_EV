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
 liste_adresse = []
 liste_nomenclature = _recherche_nomenclature_dans_bdd()
 for i in liste_nomenclature :
  liste_adresse.append(_recherche_plan_de_compte(annee_cible, i))
 dico_nomenclatures = dict(zip(liste_adresse, liste_nomenclature))
 return dico_nomenclatures

#Traitement xml par xml

def racine_transcodage(adresse) :
 ''' Créer la racine lxml du xml ''' 
 requete = requests.get(adresse + PLAN_COMPTE)
 racine = etree.fromstring(requete.content)
 enfants = racine.getchildren()
 return enfants

def extraction_nature(enfants, nomenclature) -> pd.DataFrame: 
 ''' Permet de récupérer les lignes de la branche Nature ( Nature et ContNat )'''
 nature_chapitre = enfants[0].getchildren()[0].xpath(".//*[@Code]")
 nature_compte = enfants[0].getchildren()[1].xpath(".//*[@Code]")
 liste_nature_chapitre = []
 liste_nature_compte = []

 for i in nature_chapitre :
  liste_nature_chapitre.append(i.attrib)
 df_nature_chapitre = pd.DataFrame(liste_nature_chapitre)
 df_nature_chapitre['Nomenclature'] = nomenclature

 for i in nature_compte : 
  liste_nature_compte.append(i.attrib)
 df_nature_compte = pd.DataFrame(liste_nature_compte)
 df_nature_compte['Nomenclature'] = nomenclature

 return df_nature_chapitre, df_nature_compte

def extraction_fonction(enfants, nomenclature) -> pd.DataFrame:
 ''' Permet de récupérer les lignes de la branche Fonction ( Fonction et Fonction Compte et Fonction ref, ret, machin )'''
 fonction_chapitre = enfants[1].getchildren()[0].xpath(".//*[@Code]")
 fonction_compte = enfants[1].getchildren()[1].xpath(".//*[@Code]")
 fonction_ret = enfants[1].getchildren()[2].xpath(".//*[@Code]")

 liste_fonction_chapitre = []
 liste_fonction_compte = []
 liste_fonction_ret = []

 for i in fonction_chapitre:
    liste_fonction_chapitre.append(i.attrib)
 df_fonction_chapitre = pd.DataFrame(liste_fonction_chapitre)
 df_fonction_chapitre['Nomenclature'] = nomenclature

 for i in fonction_compte:
    liste_fonction_compte.append(i.attrib)
 df_fonction_compte = pd.DataFrame(liste_fonction_compte)
 df_fonction_compte['Nomenclature'] = nomenclature

 for i in fonction_ret:
    liste_fonction_ret.append(i.attrib)
 df_fonction_ret = pd.DataFrame(liste_fonction_ret)
 df_fonction_ret['Nomenclature'] = nomenclature

 return df_fonction_chapitre, df_fonction_compte, df_fonction_ret

#Securite si ODM nous dit adieu

def _ajout_origine(df_nat, df_contnat, df_fon, df_contfon, df_fonret) -> pd.DataFrame : 
 ''' Ajout de l'origine uniquement pour la création d'un csv commun'''
 df_nat_save = df_nat.copy()
 df_nat_save['Origine'] = 'Nature'

 df_contnat_save = df_contnat.copy()
 df_contnat_save['Origine'] = 'Nature_Compte'

 df_fon_save = df_fon.copy()
 df_fon_save['Origine'] = 'Nature_Compte'

 df_contfon_save = df_contfon.copy()
 df_contfon_save['Origine'] = 'Fonction Compte'
 
 df_fonret_save = df_fonret.copy()
 df_fonret_save['Origine'] = 'Fonction RefFonctionnelles'
 return df_nat_save, df_contnat_save, df_fon_save, df_contfon_save, df_fonret_save
 
def creation_csv_commun(df_nat, df_contnat, df_fon, df_contfon, df_fonret, nomenclature) :
 ''' Permet de créer une sauvegarde en brut au cas où'''
 df_nat_2, df_contnat_2, df_fon_2, df_contfon_2, df_fonret_2 = _ajout_origine(
   df_nat, df_contnat, df_fon, df_contfon, df_fonret)
 
 df_uni = pd.concat([df_nat_2, df_contnat_2, df_fon_2, df_contfon_2, df_fonret_2], 
                    ignore_index= True)
 fichier_csv = os.path.join(DOSSIER_TRANSCODAGE, f"{nomenclature}.csv")
 df_uni.to_csv(fichier_csv, index = False)

def creation_csv_simple(nomenclature) :
 ''' Attention, c'est pas valable, ça ne fonctionne toujours pas, doit faire une selection des 4 tables voulues
 A voir si on préfère un csv par type de transco (facile) ou par type de nomenclature (un poil plus long, 
 mais on va pas oser dire que c'est difficile )'''
 conn = sqlite3.connect(BDD)
 cursor = conn.cursor()
 info = cursor.execute('''         ''')
 df = pd.DataFrame(info)
 fichier_csv = os.path.join(DOSSIER_TRANSCODAGE, f"{nomenclature}.csv")
 df.to_csv(fichier_csv, index=False)
 conn.commit()
 conn.close()

#Insertion bdd

def insertion_dans_bdd(df_nat, df_contnat, df_fon, df_contfon, df_fonret) : 
 """ insert dans une bdd les données maintenant transformées et en sort un csv à jour """
 chemin_bdd = os.path.join(DOSSIER_PARENT, BDD)
 conn = sqlite3.connect(chemin_bdd)
 df_nat.to_sql('Transcode_Nature', conn,
                    if_exists='append', index=False)
 df_contnat.to_sql('Transcode_Nature_Compte', conn,
                    if_exists='append', index=False) 
 df_fon.to_sql('Transcode_Fonction', conn,
                    if_exists='append', index=False)
 df_contfon.to_sql('Transcode_Fonction_Compte', conn,
                    if_exists='append', index=False)
 df_fonret.to_sql('Transcode_Fonction_RefFonctionnelles', conn,
                    if_exists='append', index=False)
 conn.commit()
 conn.close()

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
