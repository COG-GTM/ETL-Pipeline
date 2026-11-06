import pytest

from src.model_designer import TargetModelDesigner
from src.profiler import SchemaDetector
from src.sql_identifiers import (
    UnsafeDDLValueError,
    quote_identifier,
    safe_data_type,
    sql_comment_text,
)

MALICIOUS_COLUMN = "x); DROP TABLE t; --"
MALICIOUS_HEADER = 'notes VARCHAR(10)); DROP TABLE dim_customers; --'


def _column(name, dtype="VARCHAR(255)", nullable=True, unique=False):
    """Build a column dict shaped like SchemaDetector output."""
    return {"name": name, "source_dtype": "object", "target_dtype": dtype, "nullable": nullable, "is_unique": unique}


def _designer(table_name, columns):
    """Return a TargetModelDesigner with one source schema and a designed star schema."""
    designer = TargetModelDesigner()
    designer.add_source_schema({"detected_table_name": table_name, "columns": columns})
    designer.design_star_schema()
    return designer


@pytest.mark.parametrize("name, expected", [
    ("customer_id", "customer_id"),
    ("CustomerName", "customername"),
    ("Order$", "order$"),
])
def test_plain_names_keep_unquoted_meaning(name, expected):
    """Plain names keep PostgreSQL's unquoted (lower-cased) meaning."""
    assert quote_identifier(name) == expected


@pytest.mark.parametrize("name, expected", [("order", '"order"'), ("User", '"user"')])
def test_reserved_words_are_quoted(name, expected):
    """Reserved words are quoted so they stay identifiers."""
    assert quote_identifier(name) == expected


@pytest.mark.parametrize("name, expected", [
    (MALICIOUS_COLUMN, '"x); DROP TABLE t; --"'),
    ('a"); DROP TABLE t; --', '"a""); DROP TABLE t; --"'),
    ("shipping.carrier", '"shipping.carrier"'),
    ("$amount", '"$amount"'),
])
def test_other_names_become_one_quoted_identifier(name, expected):
    """Any other name becomes exactly one quoted identifier with embedded quotes doubled."""
    assert quote_identifier(name) == expected


@pytest.mark.parametrize("bad", ["", "a\x00b", None, 7])
def test_rejects_unrepresentable_names(bad):
    """Names PostgreSQL cannot represent are rejected."""
    with pytest.raises(UnsafeDDLValueError):
        quote_identifier(bad)


@pytest.mark.parametrize("dtype", ["INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN", "VARCHAR", "VARCHAR(30)", "VARCHAR(4000)"])
def test_allows_generated_types(dtype):
    """Types the detector generates pass through unchanged."""
    assert safe_data_type(dtype) == dtype


def test_normalizes_type_spelling_and_default():
    """Type case/spacing is normalized and None falls back to the default."""
    assert safe_data_type("numeric( 10 ,2 )") == "NUMERIC(10, 2)"
    assert safe_data_type(None) == "VARCHAR(255)"


@pytest.mark.parametrize("dtype, expected", [
    ("TEXT[]", "TEXT[]"),
    ("integer [ 3 ][]", "INTEGER[3][]"),
    ("VARCHAR(20)[]", "VARCHAR(20)[]"),
    ("INTERVAL", "INTERVAL"),
    ("interval day to second (3)", "INTERVAL DAY TO SECOND(3)"),
    ("timestamp(3) with time zone", "TIMESTAMP(3) WITH TIME ZONE"),
    ("TIME WITHOUT TIME ZONE", "TIME WITHOUT TIME ZONE"),
    ("double  precision[]", "DOUBLE PRECISION[]"),
    ("BYTEA", "BYTEA"),
    ("serial", "SERIAL"),
    ("BIGSERIAL", "BIGSERIAL"),
])
def test_allows_arrays_intervals_and_time_zones(dtype, expected):
    """Array, interval and time-zone type forms are accepted and normalized."""
    assert safe_data_type(dtype) == expected


def test_custom_types_render_in_both_generators():
    """TEXT[] and INTERVAL columns produce DDL in both generators."""
    columns = [_column("txn_id", "INTEGER"), _column("events", "TEXT[]"), _column("duration", "INTERVAL")]
    target = _designer("transactions", columns).generate_target_ddl()
    assert "    events TEXT[],\n    duration INTERVAL,\n" in target
    detected = SchemaDetector().generate_ddl({
        "detected_table_name": "transactions",
        "columns": columns,
        "primary_key_candidates": [],
        "indexes_recommended": [],
    })
    assert "    events TEXT[]" in detected and "    duration INTERVAL" in detected


def test_wide_varchar_columns_render_in_both_generators():
    """Caller-supplied VARCHAR lengths above 9999 stay valid in both generators."""
    columns = [_column("notes", "VARCHAR(10000)"), _column("blob", "varchar(10485760)")]
    target = _designer("transactions", columns).generate_target_ddl()
    assert "    notes VARCHAR(10000),\n    blob VARCHAR(10485760),\n" in target
    detected = SchemaDetector().generate_ddl({
        "detected_table_name": "transactions",
        "columns": columns,
        "primary_key_candidates": [],
        "indexes_recommended": [],
    })
    assert "    notes VARCHAR(10000)" in detected and "    blob VARCHAR(10485760)" in detected


@pytest.mark.parametrize("dtype", [
    "VARCHAR(10); DROP TABLE z", "VARCHAR(123456789)", "VARCHAR(10485761)", "VARCHAR(0)", "NUMERIC(10000)", "NUMERIC(5, 6)", "VARCHAR(10, 2)", "INTEGER DEFAULT pg_sleep(10)", "INTEGER REFERENCES t", "",
    "TEXT[]; DROP TABLE z", "INTEGER[1:2]", "INTERVAL '1 day'", "INTEGER WITH TIME ZONE", "SERIAL[]", "bigserial [ ]", "TEXT[] COLLATE \"C\"",
])
def test_rejects_injected_types(dtype):
    """Anything beyond a simple allowlisted type is rejected."""
    with pytest.raises(UnsafeDDLValueError):
        safe_data_type(dtype)


def test_line_breaks_cannot_end_a_comment():
    """Line breaks are flattened so text stays inside a -- comment."""
    assert sql_comment_text("-- a\nDROP TABLE t;\r\u2028b") == "-- a DROP TABLE t;  b"


def test_malicious_header_is_a_single_quoted_column():
    """A malicious header renders as one quoted column, not extra statements."""
    ddl = _designer("transactions", [_column("txn_id"), _column(MALICIOUS_HEADER)]).generate_target_ddl()
    assert '    "notes VARCHAR(10)); DROP TABLE dim_customers; --" VARCHAR(255),' in ddl
    assert ddl.count("CREATE TABLE") == 1


def test_one_statement_per_table_with_injected_names():
    """Injected header/file names leave exactly one CREATE TABLE statement per table."""
    import re
    designer = TargetModelDesigner()
    designer.add_source_schema({"detected_table_name": "transactions", "columns": [_column("txn_id"), _column(MALICIOUS_COLUMN)]})
    designer.add_source_schema({
        "detected_table_name": "products); DROP TABLE x; --",
        "columns": [_column("sku VARCHAR(10)); DROP TABLE dim_products; --")],
    })
    designer.design_star_schema()
    ddl = designer.generate_target_ddl()
    sql = "\n".join(line for line in ddl.splitlines() if not line.startswith("--"))
    outside_quotes = re.sub(r'"(?:[^"]|"")*"', '""', sql)
    assert outside_quotes.count(";") == outside_quotes.count("CREATE TABLE") == 2
    assert "DROP" not in outside_quotes
    assert 'CREATE TABLE "dim_products); DROP TABLE x; --" (' in ddl
    assert '    "sku VARCHAR(10)); DROP TABLE dim_products; --" VARCHAR(255),' in ddl


def test_malicious_file_name_is_quoted_in_table_and_surrogate_key():
    """A malicious file name is quoted in the table name and its surrogate key."""
    ddl = _designer('transactions"); DROP TABLE users; --', [_column("txn_id")]).generate_target_ddl()
    assert 'CREATE TABLE "fact_transactions""); DROP TABLE users; --" (' in ddl
    assert '    "fact_transactions""); DROP TABLE users; --_sk" SERIAL PRIMARY KEY' in ddl


def test_newline_in_names_cannot_escape_comments():
    """A newline in a table name cannot end a relationship comment and start SQL."""
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
    lines = designer.generate_target_ddl().splitlines()
    for i, line in enumerate(lines):
        if "DROP TABLE x" in line and not line.startswith("-- "):
            # Only allowed as the continuation of a double-quoted identifier.
            assert lines[i - 1].count('"') % 2 == 1, line
    assert any(line.startswith("-- Relationship:") and "DROP TABLE x" in line for line in lines)


def test_injected_target_dtype_is_rejected():
    """An injected target_dtype raises instead of being emitted."""
    designer = _designer("transactions", [_column("txn_id", "VARCHAR(10)); DROP TABLE z; --")])
    with pytest.raises(UnsafeDDLValueError):
        designer.generate_target_ddl()


def test_plain_model_ddl_is_unchanged():
    """Ordinary names produce the same DDL as before."""
    ddl = _designer("transactions", [_column("txn_id", "INTEGER", nullable=False), _column("note")]).generate_target_ddl()
    assert ("CREATE TABLE fact_transactions (\n    fact_transactions_sk SERIAL PRIMARY KEY,\n"
            "    txn_id INTEGER NOT NULL,\n    note VARCHAR(255),\n") in ddl


def test_schema_detector_quotes_malicious_names_everywhere():
    """SchemaDetector quotes table, column, and index identifiers."""
    schema = {
        "detected_table_name": "t; DROP TABLE a",
        "columns": [_column("id", "INTEGER", nullable=False, unique=True), _column(MALICIOUS_COLUMN)],
        "primary_key_candidates": ["id"],
        "indexes_recommended": [MALICIOUS_COLUMN],
    }
    ddl = SchemaDetector().generate_ddl(schema)
    assert 'CREATE TABLE "t; DROP TABLE a" (' in ddl
    assert '    "x); DROP TABLE t; --" VARCHAR(255)' in ddl
    assert 'CREATE INDEX "idx_t; DROP TABLE a_x); DROP TABLE t; --" ON "t; DROP TABLE a" ("x); DROP TABLE t; --");' in ddl


def test_dollar_sign_names_keep_unquoted_meaning_in_both_generators():
    """Mixed-case names with a non-leading $ fold to lower case like before."""
    target = _designer("transactions", [_column("Order$")]).generate_target_ddl()
    assert "    order$ VARCHAR(255),\n" in target
    schema = {
        "detected_table_name": "Sales$",
        "columns": [_column("Order$")],
        "primary_key_candidates": [],
        "indexes_recommended": [],
    }
    detected = SchemaDetector().generate_ddl(schema)
    assert "CREATE TABLE sales$ (" in detected
    assert "    order$ VARCHAR(255)" in detected


def test_profiled_csv_header_and_file_name_stay_quoted(tmp_path):
    """A profiled CSV with an injected header and file name yields one CREATE TABLE statement."""
    import re
    from src.profiler import SourceProfiler
    csv_path = tmp_path / "transactions); DROP TABLE t; --.csv"
    csv_path.write_text("txn_id,x INT); DROP TABLE t; --\n1,a\n2,b\n")
    schema = SchemaDetector().detect_schema(SourceProfiler().profile_auto(str(csv_path)))
    designer = TargetModelDesigner()
    designer.add_source_schema(schema)
    designer.design_star_schema(fact_table_hint="transactions")
    ddl = designer.generate_target_ddl()
    sql = "\n".join(line for line in ddl.splitlines() if not line.startswith("--"))
    outside_quotes = re.sub(r'"(?:[^"]|"")*"', '""', sql)
    assert outside_quotes.count(";") == outside_quotes.count("CREATE TABLE") == 1
    assert "DROP" not in outside_quotes
    assert 'CREATE TABLE "fact_transactions);_drop_table_t;___" (' in ddl
    assert re.search(r'^    "x INT\); DROP TABLE t; --" VARCHAR\(\d+\) NOT NULL,$', ddl, re.MULTILINE)
