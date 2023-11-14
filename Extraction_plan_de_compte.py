from lxml import etree
import pandas as pd 
import os
import sqlite3
import glob 

DOSSIER_PARENT = "."
BDD = './bdd_actes_budgetaires.db'
DOSSIER_TRANSCODAGE = "./csv_transcodage/"
DOSSIER_FICHIERS_TRANSCO = "./stockage_plan_de_compte/"
CHEMIN_PLAN_DE_COMPTE = glob.glob(os.path.join(DOSSIER_FICHIERS_TRANSCO, "*.xml"))
COLONNES_TRANSCODAGE = ['Nomenclature', 'Annee', 'Categorie', 'Code',       
                        'Lib_court', 'Libelle', 'PourEtatSeul', 
                        'Section', 'Special', 'TypeChapitre', 
                        'DEquip', 'REquip', 'DOES', 'DOIS', 
                        'DR', 'ROES', 'ROIS', 'RR', 
                        'RegrTotalise', 'Supprime', 'SupprimeDepuis']

def isolement_plan_de_compte(plan_de_compte) :
 plan_de_compte_propre = plan_de_compte.replace("\\", '/').split('/')[2].split('.')[0]
 return plan_de_compte_propre

def fusionner_annee_nomenclature(conn = 'connect') -> list:
 ''' Permet de récupérer l'année et la '''
 cursor = conn.cursor()
 cursor.execute('''SELECT DISTINCT Annee || '-' || Nomenclature 
                      FROM Transcodage''')
 annee_nomenclature = [x[0] for x in cursor.fetchall()]
 return annee_nomenclature

def parsing_fichier(chemin) : 
 with open(chemin, "rb") as fichier_ouvert:
  arbre = etree.parse(fichier_ouvert)
  racine = arbre.getroot()
  enfants = racine.getchildren()
 return enfants

def extraction_metadonnees(chemin) : 
 chemin = chemin.replace("\\", '/')
 annee = chemin.split('/')[2].split('-')[0]
 nomenclature = chemin.split('/')[2].split('-', 1)[1].split('.')[0]
 return annee, nomenclature

def extraction_nature(enfants) -> pd.DataFrame: 
 ''' Permet de récupérer les lignes de la branche Nature ( Nature et ContNat )'''
 nature_chapitre = enfants[0].getchildren()[0].xpath(".//*[@Code]")
 nature_compte = enfants[0].getchildren()[1].xpath(".//*[@Code]")
 liste_nature_chapitre = []
 liste_nature_compte = []

 for i in nature_chapitre :
  liste_nature_chapitre.append(i.attrib)
 df_nature_chapitre = pd.DataFrame(liste_nature_chapitre)
 df_nature_chapitre['Categorie'] = 'Nature'

 for i in nature_compte : 
  liste_nature_compte.append(i.attrib)
 df_nature_compte = pd.DataFrame(liste_nature_compte)
 df_nature_compte['Categorie'] = 'Nature_compte'

 return df_nature_chapitre, df_nature_compte

def extraction_fonction(enfants) -> pd.DataFrame:
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
 df_fonction_chapitre['Categorie'] = 'Fonction'

 for i in fonction_compte:
    liste_fonction_compte.append(i.attrib)
 df_fonction_compte = pd.DataFrame(liste_fonction_compte)
 df_fonction_compte['Categorie'] = 'Fonction_compte'

 for i in fonction_ret:
    liste_fonction_ret.append(i.attrib)
 df_fonction_ret = pd.DataFrame(liste_fonction_ret)
 df_fonction_ret['Categorie'] = 'Fonction_Ref'

 return df_fonction_chapitre, df_fonction_compte, df_fonction_ret

def creation_df_standard(df_nature_chapitre, df_nature_compte, 
                       df_fonction_chapitre, df_fonction_compte,
                       df_fonction_ret, annee, nomenclature) -> pd.DataFrame : 
 ''' Assemble les différents df, ajoute annee et nomenclature et standardise le schema de col pour
 qu'il corresponde à celui de la bdd'''
 df_assemblage = pd.concat([df_nature_chapitre, df_nature_compte,
                            df_fonction_chapitre, df_fonction_compte,
                            df_fonction_ret], ignore_index= True)
 df_assemblage['Nomenclature'] = nomenclature
 df_assemblage['Annee'] = annee
 df_transco = pd.DataFrame(columns= COLONNES_TRANSCODAGE)

 for i in df_assemblage.columns : #Ne conserve que les colonnes qui nous intéressent,
  if i in COLONNES_TRANSCODAGE :
   df_transco[i] = df_assemblage[i]
 return df_transco

def insertion_dans_bdd(df_transco, conn = 'connect') : 
 """ Insert dans une bdd les données maintenant transformées et en sort un csv à jour """
 df_transco.to_sql('Transcodage', conn,
                    if_exists='append', index=False)


def main() : 
 for plan_de_compte in CHEMIN_PLAN_DE_COMPTE :
  try :
   connect = sqlite3.connect(BDD)
   if isolement_plan_de_compte(plan_de_compte) not in fusionner_annee_nomenclature(connect) : 
    enfant = parsing_fichier(plan_de_compte)
    annee, nomenclature = extraction_metadonnees(plan_de_compte)
    df_nature1, df_nature2 = extraction_nature(enfant)
    df_f1, df_f2, df_f3 =  extraction_fonction(enfant)
    df_test = creation_df_standard(df_nature1, df_nature2, df_f1, df_f2, df_f3, annee, nomenclature)
    insertion_dans_bdd(df_test, connect)
   else : 
    pass 
  finally : 
   connect.commit()
   connect.close()

if __name__ == "__main__":
    main()