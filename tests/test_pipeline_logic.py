"""Tests of recovered functions in isolation, not an Airflow import test.

AST extraction skips Airflow imports and scheduler registration. Hooks are
substituted with mocks: no database, SDK or cloud credentials are involved.
"""

import ast
import contextlib
import io
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock


ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "dags" / "capstone_ecom_batch_v1.py"


def load_functions():
    tree = ast.parse(DAG_PATH.read_text())
    constants = {"TABLE_SPECS", "LOOKBACK_DAYS", "BQ_INSERT_BATCH_SIZE", "BQ_RAW_DS"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) or (
        isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id in constants for t in node.targets)
    )]
    namespace = {
        "datetime": datetime, "timedelta": timedelta, "timezone": timezone,
        "PROJECT_ID": "demo-ecommerce-project",
        "PostgresHook": MagicMock(), "BigQueryHook": MagicMock(),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(DAG_PATH), "exec"), namespace)
    return namespace


class PipelineLogicTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_functions()
        self.pg = self.ns["PostgresHook"].return_value
        self.client = self.ns["BigQueryHook"].return_value.get_client.return_value
        self.client.insert_rows_json.return_value = []
        self.ts = datetime(2026, 8, 22, 12, tzinfo=timezone.utc)

    def run_extract(self, table, rows):
        self.pg.get_records.return_value = rows
        with contextlib.redirect_stdout(io.StringIO()):
            self.ns["extract_and_merge"](table, "2026-08-22")

    def test_extraction_window_includes_seven_prior_days_and_logical_day(self):
        self.run_extract("customers", [])
        parameters = self.pg.get_records.call_args.kwargs["parameters"]
        self.assertEqual(parameters["start_ts"], datetime(2026, 8, 15))
        self.assertEqual(parameters["end_ts"], datetime(2026, 8, 23))
        self.assertEqual(parameters["end_ts"] - parameters["start_ts"], timedelta(days=8))
        # Current recovered behavior is naive: deployment must enforce UTC.
        self.assertIsNone(parameters["start_ts"].tzinfo)
        self.client.query.assert_not_called()
        self.client.insert_rows_json.assert_not_called()

    def test_each_entity_maps_source_columns_and_uses_its_primary_key(self):
        fixtures = {
            "customers": ("c1", self.ts, "FR", False, self.ts),
            "orders": ("o1", "c1", self.ts, "CREATED", "USD", Decimal("10.25"), self.ts),
            "order_items": ("oi1", "o1", "SKU-0001", 2, Decimal("5.25"), self.ts),
            "payments": ("p1", "o1", "stripe", "AUTHORIZED", self.ts, None, Decimal("10.25"), self.ts),
        }
        # Read column names from the actual published DDL, not TABLE_SPECS.
        import re
        ddl = (ROOT / "sql/bigquery/001_raw_tables.sql").read_text()
        for table, row in fixtures.items():
            with self.subTest(table=table):
                self.client.reset_mock()
                before = datetime.now(timezone.utc)
                self.run_extract(table, [row])
                after = datetime.now(timezone.utc)
                name, batch = self.client.insert_rows_json.call_args.args
                self.assertEqual(name, f"demo-ecommerce-project.raw.{table}__load_20260822")
                record = batch[0]
                block = re.search(r"\." + table + r"`\s*\((.*?)\n\)", ddl, re.S).group(1)
                schema_columns = {line.strip().split()[0] for line in block.splitlines() if line.strip()}
                self.assertEqual(set(record), schema_columns)
                self.assertEqual(record["source_updated_at"], self.ts.isoformat())
                ingested = datetime.fromisoformat(record["ingested_at"])
                self.assertLessEqual(before, ingested)
                self.assertLessEqual(ingested, after)
                pk = {"customers": "customer_id", "orders": "order_id", "order_items": "order_item_id", "payments": "payment_id"}[table]
                self.assertEqual(record[pk], row[0])
                merge = self.client.query.call_args.args[0]
                self.assertIn(f"ON T.{pk} = S.{pk}", merge)
                self.assertIn("WHEN MATCHED THEN", merge)
                self.assertIn("WHEN NOT MATCHED THEN", merge)
                self.assertEqual(self.client.query.call_count, 2)
                if table == "payments":
                    self.assertIsNone(record["settled_ts"])
                    self.assertEqual(record["amount"], 10.25)
                if table == "customers":
                    self.assertIs(record["marketing_opt_in"], False)
                if table == "orders":
                    self.assertEqual(record["total_amount"], 10.25)
                if table == "order_items":
                    self.assertEqual(record["quantity"], 2)
                    self.assertEqual(record["unit_price"], 5.25)

    def test_batches_do_not_drop_rows_at_boundary(self):
        rows = [(f"c{i}", self.ts, "FR", True, self.ts) for i in range(5001)]
        self.run_extract("customers", rows)
        batches = [c.args[1] for c in self.client.insert_rows_json.call_args_list]
        self.assertEqual([len(batch) for batch in batches], [5000, 1])
        self.assertEqual([r["customer_id"] for batch in batches for r in batch], [f"c{i}" for i in range(5001)])
        self.assertEqual(self.client.query.call_count, 2)

    def test_partial_upload_error_prevents_merge(self):
        self.client.insert_rows_json.side_effect = [[], [{"index": 0, "errors": [{"reason": "invalid"}]}]]
        rows = [(f"c{i}", self.ts, "FR", True, self.ts) for i in range(5001)]
        with self.assertRaisesRegex(RuntimeError, "batch 5000:5001"):
            self.run_extract("customers", rows)
        self.assertEqual(self.client.query.call_count, 1)
        self.assertNotIn("MERGE", self.client.query.call_args.args[0])

    def test_simulator_uses_logical_day_even_when_replayed_later(self):
        self.ns["simulate_oltp_activity"]("2026-08-22")
        cursor = self.pg.get_conn.return_value.cursor.return_value.__enter__.return_value
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(len(statements), 6)
        for statement in statements:
            self.assertIn("2026-08-22 12:00:00+00", statement)
            self.assertNotIn("NOW()", statement.upper())
        for statement in statements[:4]:
            self.assertIn("ON CONFLICT", statement)
        for statement in statements[4:]:
            self.assertIn("updated_at <=", statement)
            self.assertIn("updated_at >=", statement)


if __name__ == "__main__":
    unittest.main()
