"""Helpers for safely emitting SQL identifiers and type names in generated DDL."""

import re

DATA_TYPE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_ ]*(\(\s*\d+\s*(,\s*\d+\s*)?\))?$")

DEFAULT_DATA_TYPE = "VARCHAR(255)"


class InvalidIdentifierError(ValueError):
    """Raised when a table or column name cannot be safely used in DDL."""


def quote_identifier(name: str) -> str:
    """Quote an identifier the way ``psycopg2.extensions.quote_ident`` does.

    Embedded double quotes are doubled so that no identifier can terminate the
    quoted string and inject further DDL.

    Args:
        name: Table, column or index name, typically inferred from source data.

    Returns:
        The identifier wrapped in double quotes.

    Raises:
        InvalidIdentifierError: If the name is empty, not a string, or contains
            a NUL byte (which cannot be represented in an identifier).
    """
    if not isinstance(name, str) or not name or "\x00" in name:
        raise InvalidIdentifierError(f"Unsafe SQL identifier: {name!r}")
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def safe_data_type(data_type: str | None) -> str:
    """Return a validated SQL data type, falling back to VARCHAR(255).

    Args:
        data_type: Inferred SQL type such as ``INTEGER`` or ``VARCHAR(64)``.

    Returns:
        The data type if it matches the allowed shape, otherwise VARCHAR(255).
    """
    if isinstance(data_type, str) and DATA_TYPE_PATTERN.match(data_type.strip()):
        return data_type.strip().upper()
    return DEFAULT_DATA_TYPE
