import unittest

from src.model_designer import TargetModelDesigner
from src.profiler import SchemaDetector
from src.sql_identifiers import UnsafeDDLValueError, quote_identifier, safe_data_type, sql_comment_text

MALICIOUS_COLUMN = "x); DROP TABLE t; --"
MALICIOUS_HEADER = 'notes VARCHAR(10)); DROP TABLE dim_customers; --'


def _column(name, dtype="VARCHAR(255)", nullable=True, unique=False):
    return {"name": name, "source_dtype": "object", "target_dtype": dtype, "nullable": nullable, "is_unique": unique}


class QuoteIdentifierTest(unittest.TestCase):
    def test_plain_names_keep_unquoted_meaning(self):
        self.assertEqual(quote_identifier("customer_id"), "customer_id")
        self.assertEqual(quote_identifier("CustomerName"), "customername")

    def test_reserved_words_are_quoted(self):
        self.assertEqual(quote_identifier("order"), '"order"')
        self.assertEqual(quote_identifier("User"), '"user"')

    def test_other_names_become_one_quoted_identifier(self):
        self.assertEqual(quote_identifier(MALICIOUS_COLUMN), '"x); DROP TABLE t; --"')
        self.assertEqual(quote_identifier('a"); DROP TABLE t; --'), '"a""); DROP TABLE t; --"')
        self.assertEqual(quote_identifier("shipping.carrier"), '"shipping.carrier"')

    def test_rejects_unrepresentable_names(self):
        for bad in ("", "a\x00b", None, 7):
            with self.assertRaises(UnsafeDDLValueError):
                quote_identifier(bad)


class SafeDataTypeTest(unittest.TestCase):
    def test_allows_generated_types(self):
        for dtype in ("INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN", "VARCHAR", "VARCHAR(30)", "VARCHAR(4000)"):
            self.assertEqual(safe_data_type(dtype), dtype)
        self.assertEqual(safe_data_type("numeric( 10 ,2 )"), "NUMERIC(10, 2)")
        self.assertEqual(safe_data_type(None), "VARCHAR(255)")

    def test_rejects_injected_types(self):
        for dtype in ("VARCHAR(10); DROP TABLE z", "INTEGER DEFAULT pg_sleep(10)", "INTEGER REFERENCES t", ""):
            with self.assertRaises(UnsafeDDLValueError):
                safe_data_type(dtype)


class SqlCommentTextTest(unittest.TestCase):
    def test_line_breaks_cannot_end_a_comment(self):
        self.assertEqual(sql_comment_text("-- a\nDROP TABLE t;\r\u2028b"), "-- a DROP TABLE t;  b")


class TargetModelDesignerDDLTest(unittest.TestCase):
    def _designer(self, table_name, columns):
        designer = TargetModelDesigner()
        designer.add_source_schema({"detected_table_name": table_name, "columns": columns})
        designer.design_star_schema()
        return designer

    def test_malicious_header_is_a_single_quoted_column(self):
        ddl = self._designer("transactions", [_column("txn_id"), _column(MALICIOUS_HEADER)]).generate_target_ddl()
        self.assertIn('    "notes VARCHAR(10)); DROP TABLE dim_customers; --" VARCHAR(255),', ddl)
        self.assertEqual(ddl.count("CREATE TABLE"), 1)

    def test_malicious_file_name_is_quoted_in_table_and_surrogate_key(self):
        ddl = self._designer('transactions"); DROP TABLE users; --', [_column("txn_id")]).generate_target_ddl()
        self.assertIn('CREATE TABLE "fact_transactions""); DROP TABLE users; --" (', ddl)
        self.assertIn('    "fact_transactions""); DROP TABLE users; --_sk" SERIAL PRIMARY KEY', ddl)

    def test_newline_in_names_cannot_escape_comments(self):
        designer = TargetModelDesigner()
        designer.add_source_schema({
            "detected_table_name": "transactions",
            "columns": [_column("customer_id"), _column("amount", "DECIMAL"), _column("sold_on", "DATE")],
        })
        designer.add_source_schema({
            "detected_table_name": "customers\nDROP TABLE x; --",
            "columns": [_column("customer_id")],
        })
        designer.design_star_schema()
        ddl = designer.generate_target_ddl()
        lines = ddl.splitlines()
        for i, line in enumerate(lines):
            if "DROP TABLE x" in line and not line.startswith("-- "):
                # Only allowed as the continuation of a double-quoted identifier.
                self.assertEqual(lines[i - 1].count('"') % 2, 1, line)
        self.assertTrue(any(line.startswith("-- Relationship:") and "DROP TABLE x" in line for line in ddl.splitlines()))

    def test_injected_target_dtype_is_rejected(self):
        designer = self._designer("transactions", [_column("txn_id", "VARCHAR(10)); DROP TABLE z; --")])
        with self.assertRaises(UnsafeDDLValueError):
            designer.generate_target_ddl()

    def test_plain_model_ddl_is_unchanged(self):
        ddl = self._designer("transactions", [_column("txn_id", "INTEGER", nullable=False), _column("note")]).generate_target_ddl()
        self.assertIn("CREATE TABLE fact_transactions (\n    fact_transactions_sk SERIAL PRIMARY KEY,\n"
                      "    txn_id INTEGER NOT NULL,\n    note VARCHAR(255),\n", ddl)


class SchemaDetectorDDLTest(unittest.TestCase):
    def test_malicious_names_are_quoted_everywhere(self):
        schema = {
            "detected_table_name": "t; DROP TABLE a",
            "columns": [_column("id", "INTEGER", nullable=False, unique=True), _column(MALICIOUS_COLUMN)],
            "primary_key_candidates": ["id"],
            "indexes_recommended": [MALICIOUS_COLUMN],
        }
        ddl = SchemaDetector().generate_ddl(schema)
        self.assertIn('CREATE TABLE "t; DROP TABLE a" (', ddl)
        self.assertIn('    "x); DROP TABLE t; --" VARCHAR(255)', ddl)
        self.assertIn('CREATE INDEX "idx_t; DROP TABLE a_x); DROP TABLE t; --" ON "t; DROP TABLE a" ("x); DROP TABLE t; --");', ddl)


if __name__ == "__main__":
    unittest.main()
