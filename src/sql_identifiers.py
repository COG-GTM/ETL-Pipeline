"""Safe rendering of identifiers, types and comment text in generated PostgreSQL DDL.

Table and column names in generated DDL come from source file names and header
rows, so they must never be pasted into SQL text verbatim.
"""

import re
import unicodedata

# pg_get_keywords() catcode 'R' (reserved) and 'T' (type/function name) keywords,
# which cannot be used as bare table or column names.
POSTGRES_RESERVED_WORDS = frozenset([
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
    "authorization", "binary", "both", "case", "cast", "check", "collate", "collation",
    "column", "concurrently", "constraint", "create", "cross", "current_catalog",
    "current_date", "current_role", "current_schema", "current_time", "current_timestamp",
    "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except",
    "false", "fetch", "for", "foreign", "freeze", "from", "full", "grant", "group", "having",
    "ilike", "in", "initially", "inner", "intersect", "into", "is", "isnull", "join", "lateral",
    "leading", "left", "like", "limit", "localtime", "localtimestamp", "natural", "not",
    "notnull", "null", "offset", "on", "only", "or", "order", "outer", "overlaps", "placing",
    "primary", "references", "returning", "right", "select", "session_user", "similar", "some",
    "symmetric", "table", "tablesample", "then", "to", "trailing", "true", "union", "unique",
    "user", "using", "variadic", "verbose", "when", "where", "window", "with",
])

_ASCII_UPPER_TO_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _is_plain_identifier(name: str) -> bool:
    """Match PostgreSQL's unquoted identifier syntax, limiting non-ASCII characters to letters, marks and digits."""
    for i, ch in enumerate(name):
        if ch.isascii():
            if not (ch.isalpha() or ch == "_" or (i > 0 and (ch.isdigit() or ch == "$"))):
                return False
        elif not unicodedata.category(ch).startswith(("L",) if i == 0 else ("L", "M", "N")):
            return False
    return True

_ALLOWED_BASE_TYPES = (
    "SMALLINT", "INTEGER", "INT", "BIGINT", "SERIAL", "BIGSERIAL",
    "DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION", "FLOAT",
    "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ",
    "TEXT", "VARCHAR", "CHAR", "CHARACTER VARYING", "UUID", "JSON", "JSONB",
)
_DATA_TYPE = re.compile(
    r"(?P<base>" + "|".join(re.escape(t) for t in _ALLOWED_BASE_TYPES) + r")"
    r"(?P<args>\(\s*\d{1,4}\s*(?:,\s*\d{1,4}\s*)?\))?",
    re.IGNORECASE,
)


class UnsafeDDLValueError(ValueError):
    """Raised when a value cannot be rendered safely into generated DDL."""


def quote_identifier(name: str) -> str:
    """Render a table/column name as a PostgreSQL identifier that cannot break out of its position.

    Plain names (letters, digits, underscore, non-leading ``$``) keep their previous unquoted meaning:
    ASCII letters are case-folded to lower case, as PostgreSQL does for unquoted
    identifiers (non-ASCII letters are left as-is), and the name is only
    double-quoted when it is a reserved word.
    Any other name is emitted as a double-quoted identifier with embedded
    double quotes doubled, so it is always a single identifier token.
    """
    if not isinstance(name, str) or not name or "\x00" in name:
        raise UnsafeDDLValueError(f"Unsafe SQL identifier: {name!r}")
    if _is_plain_identifier(name):
        folded = name.translate(_ASCII_UPPER_TO_LOWER)
        if folded not in POSTGRES_RESERVED_WORDS:
            return folded
        return f'"{folded}"'
    return '"' + name.replace('"', '""') + '"'


def safe_data_type(data_type: str | None, default: str = "VARCHAR(255)") -> str:
    """Return a column type from the allowlist of simple PostgreSQL types, upper-cased."""
    if data_type is None:
        data_type = default
    if isinstance(data_type, str):
        match = _DATA_TYPE.fullmatch(data_type.strip())
        if match:
            args = re.sub(r"\s+", "", match.group("args") or "").replace(",", ", ")
            return " ".join(match.group("base").upper().split()) + args
    raise UnsafeDDLValueError(f"Unsupported SQL data type: {data_type!r}")


def sql_comment_text(value: object) -> str:
    """Flatten text for a ``--`` comment so line breaks cannot end the comment early."""
    return "".join(
        " " if unicodedata.category(ch) in ("Cc", "Cf", "Zl", "Zp") else ch
        for ch in str(value)
    )
