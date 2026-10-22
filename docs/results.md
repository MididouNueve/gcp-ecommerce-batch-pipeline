# Résultats historiques et portée des preuves

Les résultats ci-dessous sont transcrits depuis des captures fournies lors de la récupération. Les originaux restent dans l'archive privée de travail ; ils ne sont pas inclus dans ce dépôt public, car leurs terminaux contiennent des identifiants de projet et de compte.

## Backfill final

Le journal du 29 août 2026, en UTC, affiche à 19:30:13 : `finished run 7 of 7`, 42 tâches réussies, aucune tâche en échec. La clôture du DAG est visible à 19:30:14. Le dernier run affiché porte la date logique du 28 août 2026.

Les contrôles BigQuery associés affichent 4 850 clients, 19 400 commandes, 38 800 lignes de commande et 13 580 paiements. Un échantillon de dix clients du 22 août montre `created_at` et `source_updated_at` au 22 août à midi, avec `ingested_at` au 29 août à 19:29:13. Cette observation confirme la séparation entre temps métier et temps d'ingestion pour ces lignes.

Ces données comprennent un historique antérieur ; sept journées sur une base vide produiraient 350 clients, 1 400 commandes, 2 800 lignes et 980 paiements. Les chiffres historiques ne sont donc pas des valeurs attendues universelles pour ce dépôt.

## Contrôles antérieurs

Avant le dernier backfill, un contrôle des quatre clés a retourné zéro groupe dupliqué. Une réexécution d'un run manuel est documentée et les volumes affichés avant/après restent 4 500 / 18 000 / 36 000 / 12 600. L'ordre des captures SQL repose sur le contexte fourni, leurs résultats n'ayant pas d'horodatage lisible.

Une répartition antérieure des paiements affiche 11 700 `AUTHORIZED` et 900 `SETTLED`. Ce résultat ne doit pas être mélangé aux volumes du backfill final.

## Ce que ces résultats ne prouvent pas

- L'absence de doublons après le dernier backfill.
- La conformité de toutes les valeurs ou l'équilibre financier des données synthétiques.
- L'identité exacte entre le fichier Python récupéré et le fichier déployé lors des runs.
- La reproductibilité actuelle avec une version quelconque de Composer ou d'Airflow.
- L'état actuel des ressources cloud.

Les nouveaux contrôles `sql/bigquery/002_quality_checks.sql` sont fournis pour la prochaine exécution. Ils ne sont pas des contrôles historiques déjà exécutés.
