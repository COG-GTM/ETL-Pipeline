import pytest

from src.profiler.schema_detector import SchemaDetector, quote_identifier


def _col(dtype="int64", null_count=0, unique_percentage=100.0, **extra):
    info = {"dtype": dtype, "null_count": null_count, "unique_percentage": unique_percentage}
    info.update(extra)
    return info


@pytest.fixture
def detector():
    return SchemaDetector()


def test_valid_identifier_is_double_quoted():
    assert quote_identifier("customer_id") == '"customer_id"'


@pytest.mark.parametrize(
    "bad",
    ['x); DROP TABLE users;--', 'a"b', "a b", "1abc", "", "a;b", "a--b", "abc\n", "a" * 64],
)
def test_rejects_invalid_identifiers(bad):
    with pytest.raises(ValueError):
        quote_identifier(bad)


def test_table_name_from_malicious_filename_is_sanitized(detector):
    profile = {"source": '/tmp/users"); DROP TABLE users;--.csv', "columns": {}}
    assert detector.detect_schema(profile)["detected_table_name"] == "users_drop_table_users"


def test_table_name_starting_with_digit_gets_prefix(detector):
    assert detector._infer_table_name("2024-sales.csv") == "t_2024_sales"
    assert detector._infer_table_name("---.csv") == "t_unnamed"


def test_long_table_name_is_truncated_uniquely(detector):
    a = detector._infer_table_name("a" * 70 + "x.csv")
    b = detector._infer_table_name("a" * 70 + "y.csv")
    assert len(a) <= 63 and len(b) <= 63
    assert a != b


def test_ddl_quotes_all_identifiers(detector):
    profile = {
        "source": "vehicle-sales.csv",
        "columns": {
            "sale_id": _col(inferred_role="identifier"),
            "region": _col(dtype="object", unique_percentage=10.0, inferred_role="categorical",
                           stats={"max_length": 10}),
        },
    }
    ddl = detector.generate_ddl(detector.detect_schema(profile))
    assert 'CREATE TABLE "vehicle_sales" (' in ddl
    assert '    "sale_id" INTEGER NOT NULL' in ddl
    assert '    PRIMARY KEY ("sale_id")' in ddl
    assert 'CREATE INDEX "idx_vehicle_sales_region" ON "vehicle_sales" ("region");' in ddl


def test_long_table_name_index_names_do_not_collide(detector):
    schema = {
        "detected_table_name": "t" * 59,
        "columns": [],
        "primary_key_candidates": [],
        "indexes_recommended": ["first", "second"],
    }
    ddl = detector.generate_ddl(schema)
    idx_names = [line.split('"')[1] for line in ddl.splitlines() if line.startswith("CREATE INDEX")]
    assert len(idx_names) == 2
    assert idx_names[0] != idx_names[1]
    assert all(len(n) <= 63 for n in idx_names)


def test_ddl_rejects_malicious_column_header(detector):
    profile = {
        "source": "data.csv",
        "columns": {"x); DROP TABLE users;--": _col(unique_percentage=50.0)},
    }
    with pytest.raises(ValueError):
        detector.generate_ddl(detector.detect_schema(profile))


def test_ddl_rejects_malicious_index_column(detector):
    schema = {
        "detected_table_name": "t",
        "columns": [],
        "primary_key_candidates": [],
        "indexes_recommended": ['a); DROP TABLE users;--'],
    }
    with pytest.raises(ValueError):
        detector.generate_ddl(schema)
