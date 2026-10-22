import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("render_sql", ROOT / "scripts/render_sql.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class RenderSqlTests(unittest.TestCase):
    def test_all_templates_render_without_unresolved_placeholders(self):
        for path in (ROOT / "sql/bigquery").glob("*.sql"):
            with self.subTest(path=path.name):
                rendered = module.render_sql(path.read_text(), "demo-ecommerce-project")
                self.assertNotIn("${", rendered)
                self.assertIn("`demo-ecommerce-project.raw.", rendered)

    def test_rejects_empty_and_sql_fragment_project_names(self):
        for project in ["", "x", "UPPERCASE", "demo-project` ; DROP TABLE x; --", "project\n", "-project", "project-"]:
            with self.subTest(project=project):
                with self.assertRaises(ValueError):
                    module.render_sql("SELECT '${PROJECT_ID}'", project)

    def test_unknown_placeholder_is_an_error(self):
        with self.assertRaises(KeyError):
            module.render_sql("${PROJECT_ID}.${UNKNOWN}", "demo-project")


if __name__ == "__main__":
    unittest.main()
