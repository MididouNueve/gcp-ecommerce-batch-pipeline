# Limites techniques de la version retrouvée

Le DAG est publié sans correction silencieuse. Ces points viennent de sa lecture et doivent être pris en compte avant une utilisation au-delà d'une démonstration.

- **Versions non figées.** Les versions exactes Airflow, Composer et providers restent inconnues. L'import réel est à tester dans l'environnement retenu.
- **Fuseaux horaires.** La simulation utilise des timestamps UTC explicites, mais les bornes d'extraction Python et la date de départ du DAG sont naïves. La configuration UTC doit être vérifiée ; une évolution du code devrait rendre ces bornes explicites.
- **Fenêtre d'extraction.** J−7 inclus à J+1 exclu couvre huit dates. Une correction dont `updated_at` est hors fenêtre ne sera pas extraite.
- **Rejeu historique.** La source conserve son état courant, pas des snapshots par date. Le MERGE met à jour sans comparer la fraîcheur de `source_updated_at`. Ce n'est pas un mécanisme général de restauration historique, de CDC ou de résolution de conflits.
- **Montants.** Les `NUMERIC` PostgreSQL passent par `float` avant BigQuery. Une évolution destinée à des montants financiers devrait préserver la représentation décimale et tester la précision.
- **Chargement.** `get_records` et les conversions matérialisent toutes les lignes en mémoire. Les lots sont limités en nombre de lignes, pas en taille JSON. La robustesse face aux limites et délais du service n'est pas validée par les tests simulés.
- **Tables intermédiaires.** Leur nom dépend de la date logique et elles ne sont ni supprimées ni assorties d'une expiration par le DAG. Les créations/remplacements et retries demandent un test réel du comportement du service.
- **Simulation.** Les six instructions PostgreSQL utilisent l'autocommit ; une erreur peut laisser un état partiel. Les insertions ont des identifiants déterministes et `ON CONFLICT DO NOTHING`, mais cela ne constitue pas une transaction atomique de la journée.
- **Modèle synthétique.** Les totaux des commandes ne sont pas calculés à partir des lignes ; le dataset sert à démontrer le pipeline, pas une comptabilité cohérente. Les changements de statut peuvent concerner des données de la journée elle-même.
- **Qualité.** Le contrôle intégré est un simple test de communication BigQuery. Les contrôles SQL ajoutés restent manuels ; le DAG ne les appelle pas.
- **Paramétrage.** `raw` est codé en dur et la variable de projet est lue à l'import du module. Les données sont supposées uniques par clé côté source.

Des améliorations peuvent être apportées dans des commits ultérieurs, avec des tests d'intégration et une trace explicite des différences par rapport à cette version.
