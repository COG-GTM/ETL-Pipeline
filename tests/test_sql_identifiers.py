import pytest

from src.model_designer import TargetModelDesigner
from src.sql_identifiers import InvalidIdentifierError, quote_identifier, safe_data_type


def test_quote_identifier_escapes_embedded_quotes():
    assert quote_identifier('a"); DROP TABLE t; --') == '"a""); DROP TABLE t; --"'


def test_quote_identifier_rejects_empty_and_nul():
    with pytest.raises(InvalidIdentifierError):
        quote_identifier("")
    with pytest.raises(InvalidIdentifierError):
        quote_identifier("a\x00b")


def test_safe_data_type_allows_known_shapes_and_falls_back():
    assert safe_data_type("varchar(64)") == "VARCHAR(64)"
    assert safe_data_type("DECIMAL(10, 2)") == "DECIMAL(10, 2)"
    assert safe_data_type("VARCHAR(10); DROP TABLE t") == "VARCHAR(255)"
    assert safe_data_type(None) == "VARCHAR(255)"


def test_generated_ddl_neutralizes_malicious_names():
    designer = TargetModelDesigner()
    designer.add_source_schema({
        "detected_table_name": 'transactions"); DROP TABLE users; --',
        "columns": [{"name": 'x"); DROP TABLE y; --', "target_dtype": "VARCHAR(10); DROP TABLE z"}],
    })
    designer.design_star_schema()
    ddl = designer.generate_target_ddl()

    assert "DROP TABLE users; --\"" in ddl
    assert "DROP TABLE z" not in ddl
    assert ddl.count("CREATE TABLE") == 1
