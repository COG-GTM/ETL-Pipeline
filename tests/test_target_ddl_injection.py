import re

import pytest

from src.model_designer import TargetModelDesigner
from src.profiler import SchemaDetector, SourceProfiler
from src.sql_identifiers import UnsafeDDLValueError, quote_identifier, safe_data_type

PAYLOAD = "sku VARCHAR(10)); DROP TABLE x; --"


def _strip_quoted_and_comments(sql: str) -> str:
    """Remove "identifiers", 'literals' and -- comments, leaving only SQL structure."""
    sql = re.sub(r'"(?:[^"]|"")*"', '""', sql)
    sql = re.sub(r"'(?:[^']|'')*'", "''", sql)
    return re.sub(r"--[^\n]*", "", sql)


def _schema(table, columns):
    return {
        "detected_table_name": table,
        "columns": [
            {"name": c, "target_dtype": "VARCHAR(255)", "nullable": False, "is_unique": False}
            for c in columns
        ],
    }


def _ddl(*schemas):
    designer = TargetModelDesigner()
    for schema in schemas:
        designer.add_source_schema(schema)
    designer.design_star_schema(fact_table_hint="transactions")
    return designer.generate_target_ddl()


def test_malicious_column_name_stays_inside_one_quoted_identifier():
    ddl = _ddl(
        _schema("transactions", ["transaction_id", "product_id"]),
        _schema("products", ["product_id", PAYLOAD]),
    )
    structure = _strip_quoted_and_comments(ddl)

    assert '"sku VARCHAR(10)); DROP TABLE x; --" VARCHAR(255) NOT NULL' in ddl
    assert "DROP" not in structure
    assert structure.count("CREATE TABLE") == 2
    assert structure.count(";") == 2
    assert 'CREATE TABLE "dim_products" (' in ddl
    assert 'CREATE TABLE "fact_transactions" (' in ddl


def test_malicious_table_name_and_embedded_quotes_are_escaped():
    table = 'products"; DROP TABLE x; --'
    ddl = _ddl(_schema("transactions", ["transaction_id"]), _schema(table, ['a"b']))
    structure = _strip_quoted_and_comments(ddl)

    assert 'CREATE TABLE "dim_products""; DROP TABLE x; --" (' in ddl
    assert '"a""b" VARCHAR(255)' in ddl
    assert "DROP" not in structure
    assert structure.count(";") == 2


def test_newline_in_name_cannot_escape_comment_lines():
    col = "c\nDROP TABLE x; --_id"
    ddl = _ddl(
        _schema("transactions", ["transaction_id", col]),
        _schema("customers", [col]),
    )
    comments = [line for line in ddl.splitlines() if line.startswith("-- Relationship")]
    assert comments and all("\n" not in c for c in comments)
    assert "DROP" not in _strip_quoted_and_comments(ddl)


def test_malicious_header_from_csv_file(tmp_path):
    csv = tmp_path / "products.csv"
    csv.write_text(f'product_id,"{PAYLOAD}"\n1,a\n2,b\n')
    schema = SchemaDetector().detect_schema(SourceProfiler().profile_auto(str(csv)))

    ddl = _ddl(_schema("transactions", ["transaction_id"]), schema)

    assert "DROP" not in _strip_quoted_and_comments(ddl)
    assert f'"{PAYLOAD}"' in ddl


def test_unsafe_data_type_is_rejected():
    schema = _schema("transactions", ["transaction_id"])
    schema["columns"][0]["target_dtype"] = "INTEGER); DROP TABLE x; --"
    with pytest.raises(UnsafeDDLValueError):
        _ddl(schema)


@pytest.mark.parametrize("name", ["", "   ", "a\x00b", None])
def test_quote_identifier_rejects_unusable_names(name):
    with pytest.raises(UnsafeDDLValueError):
        quote_identifier(name)


@pytest.mark.parametrize("dtype", ["VARCHAR(255)", "DECIMAL", "INTEGER", "DATE", "TIMESTAMP", "BOOLEAN", "NUMERIC(10, 2)"])
def test_safe_data_type_accepts_detector_types(dtype):
    assert safe_data_type(dtype) == dtype


def test_long_source_name_still_generates_ddl():
    table = "t" * 60
    ddl = _ddl(_schema("transactions", ["transaction_id"]), _schema(table, ["c"]))
    assert f'"dim_{table}_sk" SERIAL PRIMARY KEY' in ddl
