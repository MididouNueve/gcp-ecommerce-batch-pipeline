# Exemple créé pour ce dépôt ; ce n'est pas le fichier historique final.
# Copier vers config/env.local.sh et adapter. Aucun secret dans ce fichier.
# Charger ce fichier ne crée aucune ressource et ne modifie pas gcloud.
export PROJECT_ID="your-gcp-project-id"
export REGION="europe-west9"
export BQ_RAW_DS="raw" # Le DAG retrouvé utilise ce nom en dur.
export SQL_INSTANCE="your-cloud-sql-instance"
export SQL_DB="ecommerce_db"
export SQL_USER="ecommerce_user"
export COMPOSER_ENV="your-composer-environment"

# SQL_PASSWORD et les connexions Airflow sont à configurer hors du dépôt.
# Le bucket applicatif, BQ_OPS_DS, staging et analytics ne sont pas utilisés
# par ce DAG. Aucun de ces éléments n'est nécessaire au rendu des SQL.
