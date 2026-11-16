"""Safe rendering of identifiers and types in generated PostgreSQL DDL.

Table and column names come from source file names and header rows, so they
must never be pasted into SQL text verbatim.
"""

import re

_DATA_TYPE = re.compile(
    r"(?:SMALLINT|INTEGER|INT|BIGINT|SERIAL|BIGSERIAL|DECIMAL|NUMERIC|REAL|DOUBLE PRECISION|FLOAT"
    r"|BOOLEAN|DATE|TIME|TIMESTAMP|TIMESTAMPTZ|TEXT|VARCHAR|CHAR|UUID|JSON|JSONB)"
    r"(?:\(\d{1,4}(?:,\s?\d{1,4})?\))?",
    re.IGNORECASE,
)


class UnsafeDDLValueError(ValueError):
    """Raised when a value cannot be rendered safely into generated DDL."""


def quote_identifier(name: str) -> str:
    """Return ``name`` as a double-quoted PostgreSQL identifier that is always a single token."""
    if not isinstance(name, str) or not name.strip() or "\x00" in name:
        raise UnsafeDDLValueError(f"Unsafe SQL identifier: {name!r}")
    if len(name.encode("utf-8")) > 63:
        raise UnsafeDDLValueError(f"SQL identifier longer than 63 bytes: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def safe_data_type(data_type: str | None, default: str = "VARCHAR(255)") -> str:
    """Return ``data_type`` if it is an allowlisted PostgreSQL column type."""
    value = default if data_type is None else data_type
    if isinstance(value, str) and _DATA_TYPE.fullmatch(value.strip()):
        return value.strip()
    raise UnsafeDDLValueError(f"Unsupported SQL data type: {data_type!r}")


def sql_comment_text(value: object) -> str:
    """Flatten text for a ``--`` comment so line breaks cannot end the comment early."""
    return " ".join(str(value).splitlines())
