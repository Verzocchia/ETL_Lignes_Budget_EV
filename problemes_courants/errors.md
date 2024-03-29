2024-03-20 18:52:39,490 - ERROR - Error processing file 763770: 'R_fonc_nc_nc_nc_nc__fo_fonc__fo_fonc__fonc_nc_compte'
2024-03-20 18:52:39,490 - ERROR - Error processing file 763770: 'R_fonc_nc_nc_nc_nc__fo_fonc__fo_fonc__fonc_nc_compte'
2024-03-20 18:55:44,372 - ERROR - Error processing file 765259: 'R_fonc_nc_nc_nc_nc__fo_fonc__fo_fonc__fonc_nc_compte'
2024-03-20 18:55:44,372 - ERROR - Error processing file 765259: 'R_fonc_nc_nc_nc_nc__fo_fonc__fo_fonc__fonc_nc_compte'
2024-03-20 18:57:45,699 - ERROR - Error processing file 642028: 'R_fonc_nc_nc_nc_nc__fo_fonc__fo_fonc__fonc_nc_compte'
2024-03-20 18:57:45,699 - ERROR - Error processing file 642028: 'R_fonc_nc_nc_nc_nc__fo_fonc__fo_fonc__fonc_nc_compte'
2024-03-20 18:58:15,126 - ERROR - Error processing file 617826: (psycopg2.errors.UndefinedColumn) column "MtSup_MtAPVote" of relation "bloc_budget" does not exist
2024-03-20 18:58:15,126 - ERROR - Error processing file 617826: (psycopg2.errors.UndefinedColumn) column "MtSup_MtAPVote" of relation "bloc_budget" does not exist
LINE 1: ...", "MtRAR3112", "LibCpte", "Operation", "ContOp", "MtSup_MtA...
2024-03-20 19:01:22,231 - ERROR - Error processing file 612840: (psycopg2.errors.UndefinedColumn) column "SupprimeDepuis_fonc_compte" of relation "bloc_budget" does not exist
LINE 1: ...egrTotalise_fonc_compte", "Supprime_fonc_compte", "SupprimeD...
2024-03-20 19:01:22,231 - ERROR - Error processing file 612840: (psycopg2.errors.UndefinedColumn) column "SupprimeDepuis_fonc_compte" of relation "bloc_budget" does not exist
LINE 1: ...egrTotalise_fonc_compte", "Supprime_fonc_compte", "SupprimeD...
2024-03-20 19:11:37,782 - ERROR - Error processing file 631619: (psycopg2.errors.UndefinedColumn) column "SupprimeDepuis_fonc_compte" of relation "bloc_budget" does not exist
LINE 1: ...egrTotalise_fonc_compte", "Supprime_fonc_compte", "SupprimeD...
2024-03-20 19:11:37,782 - ERROR - Error processing file 631619: (psycopg2.errors.UndefinedColumn) column "SupprimeDepuis_fonc_compte" of relation "bloc_budget" does not exist
LINE 1: ...egrTotalise_fonc_compte", "Supprime_fonc_compte", "SupprimeD...
              

old : 

786217
psycopg2.errors.UndefinedColumn) column "MtSup_RARprec" of relation "bloc_budget" does not exist
LINE 1: ...dg", "TypOpBudg", "MtSup", "MtSup_BudgetHorsRAR", "MtSup_RAR...

18478 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_ApVote" of relation "bloc_budget" does not exist
LINE 1: ...pNouv", "MtPrev", "OpBudg", "TypOpBudg", "MtSup", "MtSup_ApV...

805042 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_MtPropNouv" of relation "bloc_budget" does not exist
LINE 1: ...R3112", "OpBudg", "MtSup", "MtSup_BudgetHorsRAR", "MtSup_MtP...

818420 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_ApVote" of relation "bloc_budget" does not exist
LINE 1: ...pNouv", "MtPrev", "OpBudg", "TypOpBudg", "MtSup", "MtSup_ApV...

794105 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_MtPropNouv" of relation "bloc_budget" does not exist
LINE 1: ...OpBudg", "CaracSup", "CaracSup_Section", "MtSup", "MtSup_MtP...

818478 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_ApVote" of relation "bloc_budget" does not exist
LINE 1: ...pNouv", "MtPrev", "OpBudg", "TypOpBudg", "MtSup", "MtSup_ApV...
                                                             ^

786219 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_RARprec" of relation "bloc_budget" does not exist
LINE 1: ...acSup", "CaracSup_Section", "TypOpBudg", "MtSup", "MtSup_RAR...

805038 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_RARprec" of relation "bloc_budget" does not exist
LINE 1: ...aracSup_Section", "MtSup", "MtSup_BudgetHorsRAR", "MtSup_RAR...
                                                             ^
805039 erreur
(psycopg2.errors.UndefinedColumn) column "MtSup_MtPropNouv" of relation "bloc_budget" does not exist
LINE 1: ...edOuv", "MtReal", "MtRAR3112", "OpBudg", "MtSup", "MtSup_MtP...



SELECT                            
    relname AS "Nom de la table", 
    n_live_tup AS "Nombre de lignes"
FROM                 
    pg_stat_user_tables;









