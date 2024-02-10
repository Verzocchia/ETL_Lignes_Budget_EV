xml_to_parquet.py

    le schéma a encore des commentaires, qu'en est-il ?
    les fonctions recherche id à supprimer au profit d'un select en database lors du traitement
    _isolement_id c'est plutôt extraction_id, je trouve ton code compliqué, ça doit faire 2 lignes normalement
    travail_mtsup_ligne, travail_caracsup_ligne, il me semble que s'il y a plusieurs fois le même code, la donnée est écrasé par la dernière valeur trouvé
    xml_to_parquet, try except à mieux focaliser, ne pas le faire au global
    xml_to_parquet, séparer les infos qui sont de l'ordre du document budgétaire, du budget et des annexes. -> préparer l'arrivée de la base de données avec clés primaires, étrangères... ça aidera je pense Jamila à faire des jointures dans Superset.
    xml_to_parquet, docbase = lignes_budget, i = ligne

concat.ipynb

    Schema en pyarrow. Dans l'idéal, il faudra utiliser un ORM, pg2 ou du SQL. Pour l'instant nous avons travailler avec Parquet parce que c'était pratique, maintenant il faut que nous rendons notre code plus durable et l'adaptons à la stratégie Airflow.
    Exer en float ?
    sortir le comptage des doublons dans un autre fichier
    pour le comptage, il faut raisonner sur les documents budgétaires et pas sur les budgets
    il faut séparer ce qui est de l'explo de ce qui est du code qu'on mettra dans Airflow. 
    


crea_table_budget = """
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
        Comp ???,
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
    )
"""
