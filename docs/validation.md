# Vérifications de cette édition

Contrôles exécutés localement le 22 octobre 2026 avec Python 3.14.6 :

| Contrôle | Résultat |
|---|---|
| Huit tests unitaires hors ligne | Réussite |
| Compilation syntaxique Python du DAG, du rendu SQL et des tests | Réussite |
| Syntaxe Bash de `config/env.example.sh` | Réussite |
| Concordance des colonnes produites pour les quatre entités avec les DDL BigQuery | Réussite dans les tests simulés |
| Rendu des modèles BigQuery et rejet d'identifiants invalides | Réussite dans les tests |
| Copie du DAG par rapport à la source récupérée | Identique octet pour octet, empreinte dans `provenance.md` |
| Recherche de secrets et d'identifiants personnels dans les fichiers publiables | Aucun résultat pour les motifs contrôlés |

Les tests couvrent une fenêtre d'extraction historique, les conversions des quatre entités, une frontière de lot à 5 001 lignes, l'interruption avant MERGE après une erreur d'insertion, la date logique de simulation et le rendu des SQL. Les fonctions sont extraites par AST et exécutées avec des hooks simulés. Aucun service cloud n'a été sollicité par ces tests.

La recherche de secrets porte sur les identifiants connus du projet original, les chemins personnels, les clés privées, les formats usuels de jetons GitHub/Google et les affectations de secrets en clair. Elle est complétée par une lecture des fichiers ; ce n'est pas une garantie exhaustive contre tout format de secret.

## Validations encore nécessaires

- Installation et import réel avec les versions Airflow/providers choisies.
- Exécution des DDL sur PostgreSQL et BigQuery.
- Connexions, authentification et permissions depuis les workers.
- Tests complets du pipeline, des retries et des replays dans le cloud.
- Exécution des nouveaux contrôles de qualité après le backfill.

Le workflow GitHub Actions est configuré pour Python 3.11 et n'a pas été exécuté sur GitHub au moment de la préparation. Les résultats historiques du cloud sont documentés séparément dans `results.md`.
