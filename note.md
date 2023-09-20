Prise de note : 

- La fonction v2_nettoyage_lambda est rapide (0.0050 secondes) mais remplace les NaN par des nan. 

- Les dates ne sont pas stockées de la même manière selon les versions

- Dans les deux cas, les Nan et nan ne sont pas reconnus comme null ou Na par isna / isnull

- Comme prévu, l'insertion bdd n'était pas fonctionnelle car se basait sur le premier fichier traité, créant des conflits à chaque fois qu'un acte budgétaire possédait une colonne supplémentaires (parfois sur le même fichier), une bdd est crée en amont avec toutes les colonnes requises. 

- Conservation du fichier 18_09 test script, une partie des test est présent, c'est plus proche d'une décharge que d'une prise de notes, mais ça peut être pratique
    - Le fichier contient une fonction clean_bdd_test() pour facilement nettoyer l'intérieur de la table et recommencer certains tests

- Traitement de base : ~11sec
- Traitement lambda : ~9sec
- Traitement lambda + crea csv post boucle : ~8sec
- Séparer la créa de csv fait gagner environ une seconde (pas dégeu)
- Traitement du lambda sur l'ensemble concatené : ~6.8sec

Vitesse étape par étape : 

ouverture_gzip a pris 0.5617 secondes pour s'exécuter.
v2_parse_budget a pris 0.0040 secondes pour s'exécuter.
v2_parse_metadonnees a pris 0.0000 secondes pour s'exécuter.
v2_parse_schema a pris 0.0010 secondes pour s'exécuter.
v2_parse_date a pris 0.0000 secondes pour s'exécuter.
v2_assemblage a pris 0.0380 secondes pour s'exécuter.
v2_nettoyage_lambda a pris 0.0100 secondes pour s'exécuter.
insertion_bdd a pris 0.2132 secondes pour s'exécuter.
creation_csv a pris 0.0140 secondes pour s'exécuter.


A faire : 
- Voir si on peut grapiller des dixiemes de secondes sur l'ouverture et sur l'insertion bdd 
- placer le return true ? 
- faire des test avec json_normalize
- faire un push propre sur github
- remettre au propre (enlever les v2, changer le nom des fonctions etc.)
