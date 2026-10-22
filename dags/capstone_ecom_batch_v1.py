from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


# ============================================================
# 1) Configuration
# ============================================================

PROJECT_ID = Variable.get("gcp_project_id")

BQ_RAW_DS = "raw"
LOOKBACK_DAYS = 7

# Evite les requetes insertAll trop volumineuses vers BigQuery
BQ_INSERT_BATCH_SIZE = 5000


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# 2) Simulation OLTP
# ============================================================

def simulate_oltp_activity(ds, **context):
    """
    Simule une journee d'activite e-commerce.

    IMPORTANT :
    Les timestamps metier sont bases sur la date logique Airflow (ds)
    et NON sur NOW().

    Cela permet :
    - les executions historiques ;
    - les backfills ;
    - les extractions incrementales ;
    - la reproductibilite des executions.
    """

    pg = PostgresHook(
        postgres_conn_id="ecommerce_db_conn"
    )

    # Exemple :
    # ds = "2026-08-22"
    #
    # On place arbitrairement l'activite simulee a midi UTC.
    # Le timestamp reste ainsi dans la journee logique du DAG.
    run_date = ds
    run_ts = f"{run_date} 12:00:00+00"

    sql_statements = []

    # --------------------------------------------------------
    # 2.1 Customers
    # --------------------------------------------------------

    sql_statements.append(
        f"""
        INSERT INTO customers (
            customer_id,
            created_at,
            country,
            marketing_opt_in,
            updated_at
        )
        SELECT
            'c_' || '{run_date}' || '_' || LPAD(gs::text, 4, '0'),
            TIMESTAMPTZ '{run_ts}',
            (ARRAY['US', 'CA', 'FR', 'DE', 'GB'])[1 + (gs % 5)],
            (gs % 2 = 0),
            TIMESTAMPTZ '{run_ts}'
        FROM generate_series(1, 50) gs
        ON CONFLICT (customer_id) DO NOTHING;
        """
    )

    # --------------------------------------------------------
    # 2.2 Orders
    # --------------------------------------------------------

    sql_statements.append(
        f"""
        INSERT INTO orders (
            order_id,
            customer_id,
            order_ts,
            status,
            currency,
            total_amount,
            updated_at
        )
        SELECT
            'o_' || '{run_date}' || '_' || LPAD(gs::text, 5, '0'),
            'c_' || '{run_date}' || '_' ||
                LPAD(((gs % 50) + 1)::text, 4, '0'),

            TIMESTAMPTZ '{run_ts}'
                - (gs % 3600) * INTERVAL '1 second',

            'CREATED',
            'USD',

            (20 + (gs % 200))::numeric(12, 2),

            TIMESTAMPTZ '{run_ts}'
        FROM generate_series(1, 200) gs
        ON CONFLICT (order_id) DO NOTHING;
        """
    )

    # --------------------------------------------------------
    # 2.3 Order items
    # --------------------------------------------------------

    sql_statements.append(
        f"""
        INSERT INTO order_items (
            order_item_id,
            order_id,
            sku,
            quantity,
            unit_price,
            updated_at
        )
        SELECT
            'oi_' || '{run_date}' || '_' ||
                LPAD(gs::text, 5, '0') || '_' || item_n,

            'o_' || '{run_date}' || '_' ||
                LPAD(gs::text, 5, '0'),

            'SKU-' ||
                LPAD(((gs % 100) + 1)::text, 4, '0'),

            1 + (gs % 3),

            (5 + (gs % 50))::numeric(12, 2),

            TIMESTAMPTZ '{run_ts}'
        FROM generate_series(1, 200) gs
        CROSS JOIN (
            VALUES ('A'), ('B')
        ) AS it(item_n)
        ON CONFLICT (order_item_id) DO NOTHING;
        """
    )

    # --------------------------------------------------------
    # 2.4 Payments
    # --------------------------------------------------------

    sql_statements.append(
        f"""
        INSERT INTO payments (
            payment_id,
            order_id,
            provider,
            status,
            authorized_ts,
            settled_ts,
            amount,
            updated_at
        )
        SELECT
            'p_' || '{run_date}' || '_' ||
                LPAD(gs::text, 5, '0'),

            'o_' || '{run_date}' || '_' ||
                LPAD(gs::text, 5, '0'),

            (ARRAY['stripe', 'adyen'])[1 + (gs % 2)],

            'AUTHORIZED',

            TIMESTAMPTZ '{run_ts}',

            NULL,

            (20 + (gs % 200))::numeric(12, 2),

            TIMESTAMPTZ '{run_ts}'
        FROM generate_series(1, 200) gs
        WHERE (gs % 10) < 7
        ON CONFLICT (payment_id) DO NOTHING;
        """
    )

    # --------------------------------------------------------
    # 2.5 Late updates - Payments
    # --------------------------------------------------------

    sql_statements.append(
        f"""
        UPDATE payments
        SET
            status = 'SETTLED',
            settled_ts = TIMESTAMPTZ '{run_ts}',
            updated_at = TIMESTAMPTZ '{run_ts}'
        WHERE status = 'AUTHORIZED'

          AND updated_at >=
              TIMESTAMPTZ '{run_ts}'
              - INTERVAL '7 days'

          AND updated_at <=
              TIMESTAMPTZ '{run_ts}'

          AND (
              substring(payment_id from '.{{2}}$')::int % 20
          ) = 0;
        """
    )

    # --------------------------------------------------------
    # 2.6 Late updates - Orders
    # --------------------------------------------------------

    sql_statements.append(
        f"""
        UPDATE orders
        SET
            status = 'CANCELLED',
            updated_at = TIMESTAMPTZ '{run_ts}'
        WHERE status = 'CREATED'

          AND updated_at >=
              TIMESTAMPTZ '{run_ts}'
              - INTERVAL '7 days'

          AND updated_at <=
              TIMESTAMPTZ '{run_ts}'

          AND (
              substring(order_id from '.{{2}}$')::int % 50
          ) = 0;
        """
    )

    # --------------------------------------------------------
    # Execution PostgreSQL
    # --------------------------------------------------------

    conn = pg.get_conn()
    conn.autocommit = True

    with conn.cursor() as cur:
        for stmt in sql_statements:
            cur.execute(stmt)


# ============================================================
# 3) Configuration des extractions incrementales
# ============================================================

TABLE_SPECS = {

    # --------------------------------------------------------
    # Customers
    # --------------------------------------------------------

    "customers": {
        "pk": "customer_id",

        "select": """
            SELECT
                customer_id,
                created_at,
                country,
                marketing_opt_in,
                updated_at
            FROM customers
            WHERE updated_at >= %(start_ts)s
              AND updated_at < %(end_ts)s
        """,

        "target_cols": [
            "customer_id",
            "created_at",
            "country",
            "marketing_opt_in",
            "source_updated_at",
            "ingested_at",
        ],
    },

    # --------------------------------------------------------
    # Orders
    # --------------------------------------------------------

    "orders": {
        "pk": "order_id",

        "select": """
            SELECT
                order_id,
                customer_id,
                order_ts,
                status,
                currency,
                total_amount,
                updated_at
            FROM orders
            WHERE updated_at >= %(start_ts)s
              AND updated_at < %(end_ts)s
        """,

        "target_cols": [
            "order_id",
            "customer_id",
            "order_ts",
            "status",
            "currency",
            "total_amount",
            "source_updated_at",
            "ingested_at",
        ],
    },

    # --------------------------------------------------------
    # Order items
    # --------------------------------------------------------

    "order_items": {
        "pk": "order_item_id",

        "select": """
            SELECT
                order_item_id,
                order_id,
                sku,
                quantity,
                unit_price,
                updated_at
            FROM order_items
            WHERE updated_at >= %(start_ts)s
              AND updated_at < %(end_ts)s
        """,

        "target_cols": [
            "order_item_id",
            "order_id",
            "sku",
            "quantity",
            "unit_price",
            "source_updated_at",
            "ingested_at",
        ],
    },

    # --------------------------------------------------------
    # Payments
    # --------------------------------------------------------

    "payments": {
        "pk": "payment_id",

        "select": """
            SELECT
                payment_id,
                order_id,
                provider,
                status,
                authorized_ts,
                settled_ts,
                amount,
                updated_at
            FROM payments
            WHERE updated_at >= %(start_ts)s
              AND updated_at < %(end_ts)s
        """,

        "target_cols": [
            "payment_id",
            "order_id",
            "provider",
            "status",
            "authorized_ts",
            "settled_ts",
            "amount",
            "source_updated_at",
            "ingested_at",
        ],
    },
}


# ============================================================
# 4) Extraction incrementale + BigQuery MERGE
# ============================================================

def extract_and_merge(table_name: str, ds: str, **context):
    """
    Pipeline incremental :

    PostgreSQL
        |
        | extraction sur updated_at
        v
    BigQuery load table
        |
        | MERGE sur la cle primaire
        v
    raw.<table>

    Une fenetre glissante permet de prendre en compte les
    modifications tardives.
    """

    spec = TABLE_SPECS[table_name]

    # --------------------------------------------------------
    # Connexions
    # --------------------------------------------------------

    pg = PostgresHook(
        postgres_conn_id="ecommerce_db_conn"
    )

    bq = BigQueryHook(
        gcp_conn_id="google_cloud_default"
    )

    client = bq.get_client()

    # --------------------------------------------------------
    # Fenetre incrementale
    # --------------------------------------------------------

    run_date = datetime.strptime(
        ds,
        "%Y-%m-%d"
    ).date()

    start_date = (
        run_date
        - timedelta(days=LOOKBACK_DAYS)
    )

    end_date = (
        run_date
        + timedelta(days=1)
    )

    start_ts = datetime.combine(
        start_date,
        datetime.min.time()
    )

    end_ts = datetime.combine(
        end_date,
        datetime.min.time()
    )

    # --------------------------------------------------------
    # Extraction PostgreSQL
    # --------------------------------------------------------

    rows = pg.get_records(
        spec["select"],
        parameters={
            "start_ts": start_ts,
            "end_ts": end_ts,
        },
    )

    print(
        f"[{table_name}] extraction window: "
        f"{start_ts} -> {end_ts}"
    )

    print(
        f"[{table_name}] extracted rows: "
        f"{len(rows)}"
    )

    if not rows:
        print(
            f"[{table_name}] "
            "no changes detected"
        )
        return

    # --------------------------------------------------------
    # Conversion PostgreSQL -> JSON BigQuery
    # --------------------------------------------------------

    load_rows = []

    # ingested_at correspond volontairement a l'heure REELLE
    # d'ingestion et non a la date logique du DAG.
    now_ts = datetime.now(
        timezone.utc
    ).isoformat()

    # --------------------------------------------------------
    # Customers
    # --------------------------------------------------------

    if table_name == "customers":

        for r in rows:
            load_rows.append({
                "customer_id": r[0],

                "created_at": (
                    r[1].isoformat()
                    if r[1]
                    else None
                ),

                "country": r[2],

                "marketing_opt_in": r[3],

                "source_updated_at": (
                    r[4].isoformat()
                    if r[4]
                    else None
                ),

                "ingested_at": now_ts,
            })

    # --------------------------------------------------------
    # Orders
    # --------------------------------------------------------

    elif table_name == "orders":

        for r in rows:
            load_rows.append({
                "order_id": r[0],

                "customer_id": r[1],

                "order_ts": (
                    r[2].isoformat()
                    if r[2]
                    else None
                ),

                "status": r[3],

                "currency": r[4],

                "total_amount": (
                    float(r[5])
                    if r[5] is not None
                    else None
                ),

                "source_updated_at": (
                    r[6].isoformat()
                    if r[6]
                    else None
                ),

                "ingested_at": now_ts,
            })

    # --------------------------------------------------------
    # Order items
    # --------------------------------------------------------

    elif table_name == "order_items":

        for r in rows:
            load_rows.append({
                "order_item_id": r[0],

                "order_id": r[1],

                "sku": r[2],

                "quantity": (
                    int(r[3])
                    if r[3] is not None
                    else None
                ),

                "unit_price": (
                    float(r[4])
                    if r[4] is not None
                    else None
                ),

                "source_updated_at": (
                    r[5].isoformat()
                    if r[5]
                    else None
                ),

                "ingested_at": now_ts,
            })

    # --------------------------------------------------------
    # Payments
    # --------------------------------------------------------

    elif table_name == "payments":

        for r in rows:
            load_rows.append({
                "payment_id": r[0],

                "order_id": r[1],

                "provider": r[2],

                "status": r[3],

                "authorized_ts": (
                    r[4].isoformat()
                    if r[4]
                    else None
                ),

                "settled_ts": (
                    r[5].isoformat()
                    if r[5]
                    else None
                ),

                "amount": (
                    float(r[6])
                    if r[6] is not None
                    else None
                ),

                "source_updated_at": (
                    r[7].isoformat()
                    if r[7]
                    else None
                ),

                "ingested_at": now_ts,
            })

    else:
        raise ValueError(
            f"Unknown table: {table_name}"
        )

    # ========================================================
    # 5) BigQuery load table
    # ========================================================

    ds_nodash = ds.replace("-", "")

    load_table = (
        f"{PROJECT_ID}."
        f"{BQ_RAW_DS}."
        f"{table_name}__load_{ds_nodash}"
    )

    target_table = (
        f"{PROJECT_ID}."
        f"{BQ_RAW_DS}."
        f"{table_name}"
    )

    pk = spec["pk"]

    # --------------------------------------------------------
    # 5.1 Creation / remplacement de la load table
    # --------------------------------------------------------

    create_load_sql = f"""
        CREATE OR REPLACE TABLE `{load_table}` AS
        SELECT *
        FROM `{target_table}`
        WHERE 1 = 0
    """

    client.query(
        create_load_sql
    ).result()

    # --------------------------------------------------------
    # 5.2 Chargement par batches
    # --------------------------------------------------------

    total_rows = len(load_rows)

    print(
        f"[{table_name}] loading "
        f"{total_rows} rows into {load_table}"
    )

    for start in range(
        0,
        total_rows,
        BQ_INSERT_BATCH_SIZE
    ):

        end = min(
            start + BQ_INSERT_BATCH_SIZE,
            total_rows
        )

        batch = load_rows[start:end]

        errors = client.insert_rows_json(
            load_table,
            batch,
        )

        if errors:
            raise RuntimeError(
                f"BigQuery insert_rows_json errors "
                f"for {table_name}, "
                f"batch {start}:{end}: "
                f"{errors}"
            )

        print(
            f"[{table_name}] "
            f"loaded rows {start + 1}-{end}"
        )

    # ========================================================
    # 6) MERGE vers la table raw
    # ========================================================

    cols = spec["target_cols"]

    non_pk_cols = [
        c
        for c in cols
        if c != pk
    ]

    update_clause = ", ".join(
        [
            f"{c} = S.{c}"
            for c in non_pk_cols
        ]
    )

    insert_columns = ", ".join(
        cols
    )

    insert_values = ", ".join(
        [
            f"S.{c}"
            for c in cols
        ]
    )

    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{load_table}` S

        ON T.{pk} = S.{pk}

        WHEN MATCHED THEN
            UPDATE SET
                {update_clause}

        WHEN NOT MATCHED THEN
            INSERT (
                {insert_columns}
            )
            VALUES (
                {insert_values}
            )
    """

    client.query(
        merge_sql
    ).result()

    print(
        f"[{table_name}] MERGE completed"
    )


# ============================================================
# 7) Controle BigQuery
# ============================================================

def bq_control_check(**context):
    """
    Controle simple permettant de verifier que Composer
    communique correctement avec BigQuery.
    """

    bq = BigQueryHook(
        gcp_conn_id="google_cloud_default"
    )

    client = bq.get_client()

    sql = """
        SELECT
            CURRENT_TIMESTAMP() AS control_ts
    """

    result = list(
        client.query(sql).result()
    )

    print(
        f"BigQuery control check OK: {result}"
    )


# ============================================================
# 8) DAG
# ============================================================

with DAG(

    dag_id="capstone_ecom_batch_v1",

    start_date=datetime(
        2026,
        1,
        1
    ),

    # Le DAG reste quotidien pour permettre le backfill Airflow.
    schedule="@daily",

    # On ne veut pas qu'Airflow cree automatiquement
    # tous les runs historiques.
    catchup=False,

    max_active_runs=1,

    # Securite pendant le developpement :
    # pas d'execution automatique apres import.
    is_paused_upon_creation=True,

    default_args=DEFAULT_ARGS,

    tags=[
        "capstone",
        "batch",
        "cloudsql",
        "bigquery",
    ],

) as dag:

    # --------------------------------------------------------
    # Simulation OLTP
    # --------------------------------------------------------

    t_simulate_oltp = PythonOperator(
        task_id="simulate_oltp_activity",
        python_callable=simulate_oltp_activity,
    )

    # --------------------------------------------------------
    # Loads incrementaux
    # --------------------------------------------------------

    t_load_customers = PythonOperator(
        task_id="load_customers_raw",
        python_callable=extract_and_merge,
        op_kwargs={
            "table_name": "customers"
        },
    )

    t_load_orders = PythonOperator(
        task_id="load_orders_raw",
        python_callable=extract_and_merge,
        op_kwargs={
            "table_name": "orders"
        },
    )

    t_load_order_items = PythonOperator(
        task_id="load_order_items_raw",
        python_callable=extract_and_merge,
        op_kwargs={
            "table_name": "order_items"
        },
    )

    t_load_payments = PythonOperator(
        task_id="load_payments_raw",
        python_callable=extract_and_merge,
        op_kwargs={
            "table_name": "payments"
        },
    )

    # --------------------------------------------------------
    # Controle final
    # --------------------------------------------------------

    t_bq_control = PythonOperator(
        task_id="bq_control_check",
        python_callable=bq_control_check,
    )

    # --------------------------------------------------------
    # Dependances
    #
    # simulate
    #    |
    # customers
    #    |
    # orders
    #   /    \
    # items payments
    #   \    /
    #  bq_control
    # --------------------------------------------------------

    t_simulate_oltp >> t_load_customers

    t_load_customers >> t_load_orders

    t_load_orders >> t_load_order_items
    t_load_orders >> t_load_payments

    [
        t_load_order_items,
        t_load_payments,
    ] >> t_bq_control