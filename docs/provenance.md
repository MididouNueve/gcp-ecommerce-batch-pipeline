# Provenance de l'édition de portfolio

Cette édition a été préparée le 22 octobre 2026 à partir de code, de textes et de captures récupérés d'un projet pédagogique antérieur. Les captures originales et conversations sont conservées séparément ; elles ne font pas partie de la publication.

| Fichier ou ensemble | Origine et traitement |
|---|---|
| `dags/capstone_ecom_batch_v1.py` | Fichier complet retrouvé avec correction des dates logiques ; copie identique octet pour octet |
| `sql/postgres/001_schema.sql` | Transcription des quatre DDL et sept index, confrontée aux captures ; copie de la transcription conservée |
| `sql/bigquery/001_raw_tables.sql` | Réunion des quatre transcriptions de DDL ; la variante réussie de payments partitionnée par authorized_ts est retenue |
| `config/env.example.sh` | Nouvel exemple générique ; pas une version historique finale retrouvée |
| `sql/bigquery/002_quality_checks.sql` | Nouveaux contrôles, non exécutés dans le cloud pendant la préparation |
| `scripts/`, `tests/`, `.github/`, documentation et `.gitignore` | Ajouts pour rendre la publication lisible et vérifiable |

Empreinte SHA-256 du DAG retrouvé :

```text
6c28f1b1d347fe43be1c92990823f5d10dd08e97b9c117644281cd166c1646b9
```

La variante initiale rejetée de payments utilisait `DATE(COALESCE(settled_ts, authorized_ts))` dans le partitionnement. Elle n'est pas incluse dans les scripts à utiliser. Le schéma retenu correspond à la commande corrigée dont la création réussie est visible.

## Éléments non récupérés ou non prouvés

- Contenu intégral du fichier d'environnement après sa dernière modification.
- Versions exactes du runtime et des dépendances.
- Configuration complète des connexions Airflow, du réseau et des permissions.
- Empreinte du DAG réellement déployé lors du backfill final.
- Contrôle d'unicité réalisé après ce backfill.

La récupération ne permet donc pas d'affirmer que ce dépôt est un export intégral de l'environnement final. Les résultats historiques sont séparés des vérifications de cette édition dans [results.md](results.md) et [validation.md](validation.md).

Les identifiants de projet, adresses de comptes et secrets des pièces originales ne sont pas requis pour publier le code. Ils ont été exclus de cette édition. Aucune licence n'est déduite des captures ou des réponses d'assistants.
