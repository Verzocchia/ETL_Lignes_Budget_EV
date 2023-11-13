import requests
from bs4 import BeautifulSoup
import os 

URL_MERE = "http://odm-budgetaire.org/composants/normes/"

''' Explications dans scraping_notebook '''

def url_to_soup(url) : 
 requete = requests.get(url)
 requete_soupe = BeautifulSoup(requete.text, 'html.parser')
 return requete_soupe

def creation_chemin_variante(url_du_xml):
    nom_decoupe = url_du_xml.split('normes/')[1].replace('/', '_').split('_p')[0]
    chemin_decoupe = os.path.join('.', 'stockage_plan_de_compte', nom_decoupe)
    return chemin_decoupe

def creation_chemin_variante_save(url_du_xml) :
 '''Dans cette variante, les planDeCompte sont stockés dans le même dossier, au lieu de tous s'appeller planDeCompte,
 ils ont dans leur nom l'annee et la nomenclature'''
 nom_decoupe = url_du_xml.split('normes/')[1].replace('/', '_').split('_p')[0]
 chemin_decoupe = f'./stockage_plan_de_compte/{nom_decoupe}'
 return chemin_decoupe

def telechargement_planDeCompte(url_du_xml, chemin_fichier) : 
 requete = requests.get(url_du_xml)
 with open(chemin_fichier + '.xml', 'w') as dl_local : 
  dl_local.write(requete.text)


def main_sans_discrimination() :

 soupe_mere = url_to_soup(URL_MERE)
 soupe_annees = soupe_mere.find_all('a', string=lambda texte: '20' in texte)

 url_annees = []
 liste_url_nomenclature = []
 liste_url_xml = []

 for i in soupe_annees :
    url_annees.append(f'{URL_MERE}{i.text}')

 annee_sans_plan_de_compte = ['http://odm-budgetaire.org/composants/normes/2010/', 
                              'http://odm-budgetaire.org/composants/normes/2011/',
                              'http://odm-budgetaire.org/composants/normes/2012/']
 url_annees_propre = [year for year in url_annees if year not in annee_sans_plan_de_compte]

 for i in url_annees_propre : 
  sous_soupe = url_to_soup(i)
  ensemble_nomenclatures = sous_soupe.find_all('a', string=lambda texte: 'M' in texte)
  for nomenclature_parent in ensemble_nomenclatures : 
   liste_url_nomenclature.append(f'{i}{nomenclature_parent.text}')
   #Exemple elem de la liste : 'http://odm-budgetaire.org/composants/normes/2013/M14/'

 for nomenclature in liste_url_nomenclature : 
  soup_nom = url_to_soup(nomenclature)
  soup_nom = soup_nom.find_all('a', string=lambda texte: 'M' in texte)
  for dernier_enfant in soup_nom :
   liste_url_xml.append(f'{nomenclature}{dernier_enfant.text}planDeCompte.xml')

 for url_xml in liste_url_xml : 
   nom_fichier = creation_chemin_variante(url_xml)
   telechargement_planDeCompte(url_xml, nom_fichier)

if __name__ == "__main__":
    main_sans_discrimination()

