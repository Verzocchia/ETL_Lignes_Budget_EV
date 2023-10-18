import requests
from lxml import etree 
import pandas as pd 
import os

URL_MERE_PLAN_DE_COMPTE = "http://odm-budgetaire.org/composants/normes/"
PLAN_COMPTE = "planDeCompte.xml"

def recherche_plan_de_compte(annee, nomenclature) : 
 url_dossier = URL_MERE_PLAN_DE_COMPTE + f"{annee}/" + f"{nomenclature.split('-')[0]}/" + f"{nomenclature.split('-')[1]}/"
 return url_dossier

def racine_transcodage(adresse) : 
 Requete = requests.get(adresse + PLAN_COMPTE)
 Racine = etree.fromstring(Requete.content)
 Enfants = Racine.getchildren()
 return Enfants

def extraction_nature(Enfants) : 
 Nature_Chapitre = Enfants[0].getchildren()[0].xpath(".//*[@Code]")
 Nature_Compte = Enfants[0].getchildren()[1].xpath(".//*[@Code]")
 Liste_Nature = []

 for i in Nature_Chapitre :
  i.attrib['Origine'] = 'Nature_Chapitre'
  Liste_Nature.append(i.attrib)

 for i in Nature_Compte : 
  i.attrib['Origine'] = 'Nature_Compte'
  Liste_Nature.append(i.attrib)

 df_Nature = pd.DataFrame(Liste_Nature)
 return df_Nature

def extraction_fonction(Enfants) :
 Fonction_chapitre = Enfants[1].getchildren()[0].xpath(".//*[@Code]")
 Fonction_compte = Enfants[1].getchildren()[1].xpath(".//*[@Code]")
 Fonction_ret = Enfants[1].getchildren()[2].xpath(".//*[@Code]")
 Liste_Fonction = []
 for i in Fonction_chapitre:
    i.attrib['Origine'] = 'Fonction_Chapitre'
    Liste_Fonction.append(i.attrib)

 for i in Fonction_compte:
    i.attrib['Origine'] = 'Fonction_Compte'
    Liste_Fonction.append(i.attrib)

 for i in Fonction_ret:
    i.attrib['Origine'] = 'Fonction_Ret'
    Liste_Fonction.append(i.attrib)
 df_Fonction = pd.DataFrame(Liste_Fonction)
 return df_Fonction

def creation_df_nomenclature(df_nat, df_fonc, nomenclature) :
 df_transcodage = pd.concat([df_nat, df_fonc], ignore_index= True)
 df_transcodage['Nomenclature'] = nomenclature
 return df_transcodage
 
def creation_csv(dataframe, nomenclature) :
 fichier_csv = os.path.join("./csv_transcodage/", f"{nomenclature}.csv")
 dataframe.to_csv(fichier_csv, index = False)
 dataframe

def xmltocsv(annee, nomenclature) :
 adresse = recherche_plan_de_compte(annee, nomenclature)
 enfants = racine_transcodage(adresse)
 enfant_nature = extraction_nature(enfants)
 enfant_fonction = extraction_fonction(enfants)
 df = creation_df_nomenclature(enfant_nature, enfant_fonction, nomenclature)
 creation_csv(df, nomenclature)
 