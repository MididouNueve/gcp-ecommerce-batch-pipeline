-- Contrôles ajoutés pour la publication, jamais exécutés pendant la récupération.
-- Rendre ce modèle avec scripts/render_sql.py avant utilisation.

SELECT 'customers' AS table_name, COUNT(*) AS row_count, COUNT(DISTINCT customer_id) AS distinct_keys, COUNTIF(customer_id IS NULL) AS null_keys, COUNTIF(source_updated_at IS NULL) AS missing_source_timestamp, COUNTIF(ingested_at IS NULL) AS missing_ingestion_timestamp FROM `${PROJECT_ID}.${BQ_RAW_DS}.customers`
UNION ALL
SELECT 'orders' AS table_name, COUNT(*) AS row_count, COUNT(DISTINCT order_id) AS distinct_keys, COUNTIF(order_id IS NULL) AS null_keys, COUNTIF(source_updated_at IS NULL) AS missing_source_timestamp, COUNTIF(ingested_at IS NULL) AS missing_ingestion_timestamp FROM `${PROJECT_ID}.${BQ_RAW_DS}.orders`
UNION ALL
SELECT 'order_items' AS table_name, COUNT(*) AS row_count, COUNT(DISTINCT order_item_id) AS distinct_keys, COUNTIF(order_item_id IS NULL) AS null_keys, COUNTIF(source_updated_at IS NULL) AS missing_source_timestamp, COUNTIF(ingested_at IS NULL) AS missing_ingestion_timestamp FROM `${PROJECT_ID}.${BQ_RAW_DS}.order_items`
UNION ALL
SELECT 'payments' AS table_name, COUNT(*) AS row_count, COUNT(DISTINCT payment_id) AS distinct_keys, COUNTIF(payment_id IS NULL) AS null_keys, COUNTIF(source_updated_at IS NULL) AS missing_source_timestamp, COUNTIF(ingested_at IS NULL) AS missing_ingestion_timestamp FROM `${PROJECT_ID}.${BQ_RAW_DS}.payments`;

ASSERT (SELECT COUNT(*) = COUNT(DISTINCT customer_id) FROM `${PROJECT_ID}.${BQ_RAW_DS}.customers`) AS 'customers: cle nulle ou dupliquee';

ASSERT (SELECT COUNT(*) = COUNT(DISTINCT order_id) FROM `${PROJECT_ID}.${BQ_RAW_DS}.orders`) AS 'orders: cle nulle ou dupliquee';

ASSERT (SELECT COUNT(*) = COUNT(DISTINCT order_item_id) FROM `${PROJECT_ID}.${BQ_RAW_DS}.order_items`) AS 'order_items: cle nulle ou dupliquee';

ASSERT (SELECT COUNT(*) = COUNT(DISTINCT payment_id) FROM `${PROJECT_ID}.${BQ_RAW_DS}.payments`) AS 'payments: cle nulle ou dupliquee';

ASSERT (SELECT COUNT(*) = 0 FROM `${PROJECT_ID}.${BQ_RAW_DS}.orders` c LEFT JOIN `${PROJECT_ID}.${BQ_RAW_DS}.customers` p ON c.customer_id = p.customer_id WHERE c.customer_id IS NULL OR p.customer_id IS NULL) AS 'orders: reference absente vers customers';

ASSERT (SELECT COUNT(*) = 0 FROM `${PROJECT_ID}.${BQ_RAW_DS}.order_items` c LEFT JOIN `${PROJECT_ID}.${BQ_RAW_DS}.orders` p ON c.order_id = p.order_id WHERE c.order_id IS NULL OR p.order_id IS NULL) AS 'order_items: reference absente vers orders';

ASSERT (SELECT COUNT(*) = 0 FROM `${PROJECT_ID}.${BQ_RAW_DS}.payments` c LEFT JOIN `${PROJECT_ID}.${BQ_RAW_DS}.orders` p ON c.order_id = p.order_id WHERE c.order_id IS NULL OR p.order_id IS NULL) AS 'payments: reference absente vers orders';

SELECT status, COUNT(*) AS payment_count FROM `${PROJECT_ID}.${BQ_RAW_DS}.payments` GROUP BY status ORDER BY status;

-- Les assertions passent aussi sur des tables vides : inspecter row_count.
-- Elles ne valident pas la precision des montants ou toutes les regles metier.
