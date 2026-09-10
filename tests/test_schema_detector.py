import unittest

from src.profiler.schema_detector import SchemaDetector, quote_identifier


def _col(dtype="int64", null_count=0, unique_percentage=100.0, **extra):
    info = {"dtype": dtype, "null_count": null_count, "unique_percentage": unique_percentage}
    info.update(extra)
    return info


class QuoteIdentifierTests(unittest.TestCase):
    def test_valid_identifier_is_double_quoted(self):
        self.assertEqual(quote_identifier("customer_id"), '"customer_id"')

    def test_rejects_sql_metacharacters(self):
        for bad in ['x); DROP TABLE users;--', 'a"b', "a b", "1abc", "", "a;b", "a--b"]:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    quote_identifier(bad)

    def test_rejects_overlong_identifier(self):
        with self.assertRaises(ValueError):
            quote_identifier("a" * 64)


class SchemaDetectorDdlTests(unittest.TestCase):
    def setUp(self):
        self.detector = SchemaDetector()

    def test_table_name_from_malicious_filename_is_sanitized(self):
        profile = {"source": '/tmp/users"); DROP TABLE users;--.csv', "columns": {}}
        schema = self.detector.detect_schema(profile)
        self.assertEqual(schema["detected_table_name"], "users_drop_table_users")

    def test_table_name_starting_with_digit_gets_prefix(self):
        self.assertEqual(self.detector._infer_table_name("2024-sales.csv"), "t_2024_sales")
        self.assertEqual(self.detector._infer_table_name("---.csv"), "t_unnamed")

    def test_ddl_quotes_all_identifiers(self):
        profile = {
            "source": "vehicle-sales.csv",
            "columns": {
                "sale_id": _col(inferred_role="identifier"),
                "region": _col(dtype="object", unique_percentage=10.0, inferred_role="categorical",
                               stats={"max_length": 10}),
            },
        }
        ddl = self.detector.generate_ddl(self.detector.detect_schema(profile))
        self.assertIn('CREATE TABLE "vehicle_sales" (', ddl)
        self.assertIn('    "sale_id" INTEGER NOT NULL', ddl)
        self.assertIn('    PRIMARY KEY ("sale_id")', ddl)
        self.assertIn('CREATE INDEX "idx_vehicle_sales_region" ON "vehicle_sales" ("region");', ddl)

    def test_ddl_rejects_malicious_column_header(self):
        profile = {
            "source": "data.csv",
            "columns": {"x); DROP TABLE users;--": _col(unique_percentage=50.0)},
        }
        with self.assertRaises(ValueError):
            self.detector.generate_ddl(self.detector.detect_schema(profile))

    def test_ddl_rejects_malicious_index_column(self):
        schema = {
            "detected_table_name": "t",
            "columns": [],
            "primary_key_candidates": [],
            "indexes_recommended": ['a); DROP TABLE users;--'],
        }
        with self.assertRaises(ValueError):
            self.detector.generate_ddl(schema)


if __name__ == "__main__":
    unittest.main()
