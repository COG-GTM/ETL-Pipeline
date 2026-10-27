import os
import tempfile
import unittest

from src.model_designer.target_model_designer import TargetModelDesigner
from src.profiler.schema_detector import SchemaDetector
from src.profiler.source_profiler import SourceProfiler
from src.sql_identifiers import quote_identifier, sanitize_identifier

MALICIOUS_HEADER = "id INTEGER); DROP TABLE customers; --"


def _profile_csv(file_name: str, content: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, file_name)
        with open(path, "w") as fh:
            fh.write(content)
        return SourceProfiler().profile_csv(path)


class SqlIdentifierTest(unittest.TestCase):
    def test_sanitize_identifier(self):
        self.assertEqual(sanitize_identifier(MALICIOUS_HEADER), "id_integer_drop_table_customers")
        self.assertEqual(sanitize_identifier("Vendor-Feed 2024"), "vendor_feed_2024")
        self.assertEqual(sanitize_identifier("2024_sales"), "_2024_sales")
        self.assertEqual(sanitize_identifier("';--", fallback="column"), "column")
        self.assertEqual(sanitize_identifier("supplier.id"), "supplier_id")

    def test_sanitize_identifier_truncates_with_distinct_hash(self):
        a = sanitize_identifier("a" * 100)
        b = sanitize_identifier("a" * 101)
        self.assertLessEqual(len(a), 63)
        self.assertNotEqual(a, b)

    def test_quote_identifier_escapes_embedded_quotes(self):
        self.assertEqual(quote_identifier("orders"), '"orders"')
        self.assertEqual(quote_identifier('x"); DROP TABLE y; --'), '"x""); DROP TABLE y; --"')
        with self.assertRaises(ValueError):
            quote_identifier("")


class SchemaDetectorDdlTest(unittest.TestCase):
    def test_malicious_header_and_file_name_become_single_identifiers(self):
        profile = _profile_csv(
            'evil"); DROP TABLE t; --.csv',
            f'"{MALICIOUS_HEADER}",region\n1,north\n2,south\n3,north\n',
        )
        detector = SchemaDetector()
        schema = detector.detect_schema(profile)
        ddl = detector.generate_ddl(schema)

        self.assertEqual(schema["detected_table_name"], "evil_drop_table_t")
        self.assertEqual(schema["columns"][0]["name"], "id_integer_drop_table_customers")
        self.assertEqual(schema["columns"][0]["source_name"], MALICIOUS_HEADER)
        self.assertNotIn(";", ddl.replace(");", ""))
        self.assertNotIn("--", ddl)
        self.assertEqual(ddl.count("CREATE TABLE"), 1)
        self.assertTrue(ddl.startswith('CREATE TABLE "evil_drop_table_t" ('))

    def test_generate_ddl_quotes_raw_schema_names(self):
        schema = {
            "detected_table_name": "t; DROP TABLE x",
            "columns": [
                {"name": 'a"b', "target_dtype": "INTEGER", "nullable": False, "is_unique": True},
            ],
            "primary_key_candidates": ['a"b'],
            "indexes_recommended": ['a"b'],
        }
        ddl = SchemaDetector().generate_ddl(schema)
        self.assertIn('CREATE TABLE "t; DROP TABLE x" (', ddl)
        self.assertIn('    "a""b" INTEGER NOT NULL', ddl)
        self.assertIn('PRIMARY KEY ("a""b")', ddl)
        self.assertIn('CREATE INDEX "idx_t_drop_table_x_a_b" ON "t; DROP TABLE x" ("a""b");', ddl)

    def test_duplicate_sanitized_headers_get_unique_names(self):
        profile = _profile_csv("dupes.csv", "a-b,a b,a_b\n1,2,3\n")
        schema = SchemaDetector().detect_schema(profile)
        self.assertEqual([c["name"] for c in schema["columns"]], ["a_b", "a_b_2", "a_b_3"])


class TargetModelDdlTest(unittest.TestCase):
    def test_target_ddl_quotes_identifiers(self):
        designer = TargetModelDesigner()
        designer.add_source_schema({
            "detected_table_name": "transactions",
            "columns": [{"name": 'x"); DROP TABLE y; --', "target_dtype": "INTEGER", "nullable": True}],
        })
        designer.design_star_schema()
        ddl = designer.generate_target_ddl()
        self.assertIn('CREATE TABLE "fact_transactions" (', ddl)
        self.assertIn('"fact_transactions_sk" SERIAL PRIMARY KEY', ddl)
        self.assertIn('    "x""); DROP TABLE y; --" INTEGER', ddl)

    def test_sources_with_same_sanitized_name_stay_separate(self):
        designer = TargetModelDesigner()
        for _ in range(2):
            designer.add_source_schema({
                "detected_table_name": "a_b",
                "columns": [{"name": "v", "target_dtype": "INTEGER", "nullable": True}],
            })
        model = designer.design_star_schema()
        names = [model["fact_table"]["name"]] + [d["name"] for d in model["dimension_tables"]]
        self.assertEqual(sorted(names), ["dim_a_b_2", "fact_a_b"])
        self.assertEqual(len(model["fact_table"]["columns"]), 1)

    def test_long_target_table_names_stay_distinct_within_limit(self):
        designer = TargetModelDesigner()
        for suffix in ("one", "two", "three"):
            designer.add_source_schema({
                "detected_table_name": "a" * 59 + suffix if suffix != "three" else "a" * 63,
                "columns": [{"name": "v", "target_dtype": "INTEGER", "nullable": True}],
            })
        model = designer.design_star_schema()
        names = [model["fact_table"]["name"]] + [d["name"] for d in model["dimension_tables"]]
        self.assertEqual(len(set(names)), 3)
        self.assertTrue(all(len(n) <= 63 for n in names))
        self.assertTrue(all(len(n) <= 63 for n in (designer._surrogate_key_name(n) for n in names)))

    def test_source_columns_do_not_clash_with_audit_columns(self):
        designer = TargetModelDesigner()
        designer.add_source_schema({
            "detected_table_name": "transactions",
            "columns": [
                {"name": "etl_batch_id", "target_dtype": "INTEGER", "nullable": True},
                {"name": "fact_transactions_sk", "target_dtype": "INTEGER", "nullable": True},
            ],
        })
        designer.add_source_schema({
            "detected_table_name": "customers",
            "columns": [{"name": "effective_date", "target_dtype": "INTEGER", "nullable": True}],
        })
        model = designer.design_star_schema()
        self.assertEqual([c["name"] for c in model["fact_table"]["columns"]], ["etl_batch_id_2", "fact_transactions_sk_2"])
        self.assertEqual([c["name"] for c in model["dimension_tables"][0]["columns"]], ["effective_date_2"])


if __name__ == "__main__":
    unittest.main()
