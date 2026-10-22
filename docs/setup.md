# Préparer une nouvelle exécution

Ce guide est un ajout de publication. Ce n'est pas une transcription de la procédure historique complète. Il suppose un projet de test disposant déjà de Cloud SQL PostgreSQL, BigQuery et d'un environnement Airflow compatible. Il ne crée pas automatiquement l'infrastructure.

## 1. Environnement d'exécution

Le code retrouvé cible l'API Airflow 2 et les providers Google et PostgreSQL. La version exacte de Composer, d'Airflow et des providers historiquement utilisés n'a pas été retrouvée ; l'alias de création était `composer-2-airflow-2`. La compatibilité avec Airflow 3 n'a pas été validée.

Pour Composer, inspecter les versions et dépendances de l'environnement choisi avant tout ajout de paquet. Ne pas réinstaller arbitrairement Airflow dans Composer. Pour un environnement local Airflow 2 séparé, choisir et figer ensemble Python, Airflow et les providers avec les contraintes officielles. La [documentation d'installation Airflow 2](https://airflow.apache.org/docs/apache-airflow/2.10.5/installation/installing-from-pypi.html) décrit ce mécanisme ; ce lien ne prouve pas que la version 2.10.5 était utilisée dans le projet.

Le dépôt ne fournit pas de faux verrou de dépendances historique. Les tests hors ligne n'ont besoin que de la bibliothèque standard Python.

## 2. Configuration locale

Depuis la racine du dépôt :

```bash
cp config/env.example.sh config/env.local.sh
# Modifier les valeurs dans config/env.local.sh, puis :
source config/env.local.sh
```

Le fichier local est ignoré par Git. Il sert aux commandes du guide ; le DAG ne lit pas ces variables shell. Le dataset cible du DAG est fixé à `raw`.

Configurer séparément dans Airflow :

| Élément | Valeur ou rôle |
|---|---|
| Variable `gcp_project_id` | Identifiant de votre projet GCP, nécessaire dès l'import du DAG |
| Connexion `ecommerce_db_conn` | Connexion PostgreSQL vers la base source : hôte accessible depuis les workers, port, base, utilisateur et secret |
| Connexion `google_cloud_default` | Authentification Google appropriée à l'environnement, de préférence son identité de service |
| Fuseau Airflow et session PostgreSQL | UTC pour cette version : les bornes d'extraction Python sont sans fuseau explicite |

Un proxy démarré sur le poste local ne rend pas PostgreSQL accessible aux workers Composer. Vérifier la connectivité depuis ces workers et les droits SQL nécessaires à la simulation et à l'extraction. L'identité Google doit pouvoir exécuter des jobs BigQuery et créer/écrire/lire les tables de chargement et cibles du dataset. La configuration réseau et IAM complète n'a pas été récupérée.

## 3. Schéma source

Dans une base PostgreSQL de test accessible à votre client `psql`, avec `PGHOST`, `PGPORT`, `PGDATABASE` et `PGUSER` déjà configurés :

```bash
psql -X -W -v ON_ERROR_STOP=1 -f sql/postgres/001_schema.sql
```

`-W` demande le mot de passe sans l'inscrire dans une commande. Exécuter le schéma avec un utilisateur autorisé à créer les tables et index. Le DAG utilise des noms de tables sans schéma : vérifier également le `search_path` de la connexion Airflow.

## 4. Schéma BigQuery

Créer le dataset `raw` s'il n'existe pas déjà, dans la région retenue :

```bash
bq --project_id="$PROJECT_ID" mk --dataset --location="$REGION" "$PROJECT_ID:raw"
```

Rendre les modèles SQL localement, les lire, puis les exécuter dans le projet de test :

```bash
mkdir -p build
python scripts/render_sql.py sql/bigquery/001_raw_tables.sql > build/raw_tables.sql
bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false < build/raw_tables.sql
```

Le rendu remplace uniquement `${PROJECT_ID}` et `${BQ_RAW_DS}` et fixe ce dernier à `raw`. Il n'exécute pas de SQL. Les modèles ne doivent pas être envoyés directement à BigQuery sans substitution.

Les commandes `IF NOT EXISTS` ne migrent pas une table déjà présente : comparer les schémas et partitions si le dataset existe. Les tables `orders` et `payments` utilisent respectivement `DATE(order_ts)` et `DATE(authorized_ts)`. Voir les [expressions de partitionnement BigQuery](https://cloud.google.com/bigquery/docs/creating-partitioned-tables).

## 5. Charger le DAG

Vérifier d'abord les connexions, la variable, les versions et les schémas. Dans Composer, les fichiers DAG sont stockés dans le bucket associé à l'environnement ; c'est ce bucket qu'il faut utiliser. Voir la [procédure officielle de gestion des DAGs](https://cloud.google.com/composer/docs/composer-2/manage-dags).

Exemple pour un environnement existant :

```bash
gcloud composer environments storage dags import \
  --project="$PROJECT_ID" \
  --environment="$COMPOSER_ENV" \
  --location="$REGION" \
  --source=dags/capstone_ecom_batch_v1.py
```

Contrôler les erreurs d'import dans Airflow. Le DAG est configuré en pause lors de sa création ; cette option ne remet pas en pause un DAG déjà existant. Vérifier son état avant modification. La date de départ du code retrouvé est le 1er janvier 2026.

## 6. Valider dans un projet de test

1. Choisir une journée logique postérieure ou égale à la date de départ ; exécuter le DAG et vérifier les six tâches.
2. Vérifier les dates métier dans PostgreSQL et BigQuery, les valeurs nulles, les clés et les relations entre tables.
3. Exécuter les contrôles ajoutés ci-dessous et conserver les résultats avec leur date.
4. Rejouer la même journée ; vérifier les comptes, l'unicité et les valeurs métier. `ingested_at` change lors d'un nouveau MERGE : l'idempotence ne signifie pas une identité de toutes les colonnes.
5. Sur une base dédiée, tester ensuite plusieurs dates dans l'ordre chronologique. Pour un backfill, adapter la commande à la version réelle d'Airflow et commencer par un petit intervalle.

```bash
python scripts/render_sql.py sql/bigquery/002_quality_checks.sql > build/quality_checks.sql
bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false < build/quality_checks.sql
```

Ce script lit les données et contient des assertions ; il ne les corrige pas. Les nombres historiques du README ne sont pas des seuils à imposer à une nouvelle base. Sur une base vide, la simulation ajoute par journée 50 clients, 200 commandes, 400 lignes et 140 paiements, sous réserve d'une exécution réussie.

Les tables `__load_YYYYMMDD` restent présentes après le run. Définir une rétention adaptée une fois les résultats validés. L'usage de Cloud SQL, Composer et BigQuery nécessite un suivi des ressources et des coûts ; aucune suppression automatique n'est prévue ici.
