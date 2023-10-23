# Création BDD 
Crée la table bdd qui accueillera les données (passage nécessaire à duck db)

Stratégie : 
- Crée la table Ligne Budget (sous le nom de actes_budgetaires), table vide mais composées des colonnes qui sont dans la liste COLONNES_A_CONSERVER dans le script_etl_gz. Ca permet d'insérer, de façon sûre, les lignes des fichiers qui seront traités dans cette table. La principale limite était avec les colonnes CaracSup et MtSup, il faut en créer suffisamment (en amont) pour accueillir la possibilité qu'une ligne budgétaire possède plusieurs CaracSup et/ou plusieurs MtSup
- Crée les tables de transcodage, une par spécificité : nature, contnat, fonction, cont_fonction et fonctionret. Ajoute une colonne nomenclature pour aller chercher dedans au besoin d'un transcodage spécifique lors d'un join. 

# Script GZ
Permet l'extraction des données depuis un dossier todo (modifiable dans les premières lignes du script)
- Premièrement, le script va chercher dans le dossier "todo" les fichiers qui ne sont pas déjà dans la table actes_budgetaire via la colonne ID (correspondant au nom du fichier) pour éviter les doublons

Pour chaque fichier dans cette liste sûre : 
- Parsing et extraction des métadonnées et du contenu des lignes budgétaires via xmltodict 
- Insertion de ces données dans un DataFrame contenant au préalable les colonnes que nous souhaitons (COLONNE_A_CONSERVER)
- Ce DataFrame va ensuite être inséré dans la bdd 

A la toute fin du script, quand tous les fichiers ont été traités, la table actes_budgetaires maintenant mis à jour va être copié sous forme de CSV

# 16_10 transcodages & script_transcodage
- Le script va chercher dans la bdd les nomenclatures présentes (dans l'idéal prendre toutes celles sur le site), il est aussi possible de le faire que pour une nomenclature en particulier
- Pour chaque nomenclature, le script va scraper le contenu le plan de compte correspond sur le site odm-budgetaire. 
- Ce plan de compte sera parsé via lxml (xmltodict ne peux pas correctement récupérer ces informations)
- A partir de l'arbre xml, 5 DataFrames vont être crées, correspondant aux différents types de codage (nature etc.)
- Ces df vont être insérés dans les tables correspondantes de la bdd, avec l'ajout d'une colonne nomenclature pour s'y retrouver
- Une copie en brut, sous forme de CSV est ensuite créer pour chaque nomenclature au cas où. (Les fonctions devront être revues et mieux faites, il faut changer de stratégie)
