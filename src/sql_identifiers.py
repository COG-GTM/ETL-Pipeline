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

_PLAIN_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")

_ALLOWED_BASE_TYPES = (
    "SMALLINT", "INTEGER", "INT", "BIGINT",
    "DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION", "FLOAT", "MONEY",
    "BOOLEAN", "DATE", "TIMESTAMPTZ", "TIMETZ",
    "TEXT", "VARCHAR", "CHARACTER VARYING", "CHARACTER", "CHAR", "UUID", "JSON", "JSONB",
    "BYTEA", "INET", "CIDR", "MACADDR", "XML",
)
_INTERVAL_FIELDS = (
    "YEAR TO MONTH", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND",
    "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE TO SECOND",
    "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
)


def _keywords(words):
    """Build regex alternatives for SQL keywords, allowing flexible spacing."""
    return "|".join(r"\s+".join(map(re.escape, w.split())) for w in words)


# SERIAL/BIGSERIAL are pseudo-types without array forms.
_DATA_TYPE = re.compile(
    r"(?:BIG)?SERIAL"
    r"|(?:(?:"
    r"(?:" + _keywords(_ALLOWED_BASE_TYPES) + r")(?:\s*\(\s*\d{1,8}\s*(?:,\s*\d{1,4}\s*)?\))?"
    r"|(?:TIMESTAMP|TIME)(?:\s*\(\s*\d\s*\))?(?:\s+WITH(?:OUT)?\s+TIME\s+ZONE)?"
    r"|INTERVAL(?:\s+(?:" + _keywords(_INTERVAL_FIELDS) + r"))?(?:\s*\(\s*\d\s*\))?"
    r")"
    r"(?:\s*\[\s*\d{0,9}\s*\]){0,6})",
    re.IGNORECASE,
)


def _normalize_type(data_type: str) -> str:
    """Normalize casing and spacing in a validated PostgreSQL data type."""
    text = " ".join(data_type.split()).upper()
    text = re.sub(r"\s*([(\[])\s*", r"\1", text)
    text = re.sub(r"\s*([)\]])", r"\1", text)
    return re.sub(r"\s*,\s*", ", ", text)


_TYPE_LIMITS = {
    "VARCHAR": 10485760, "CHARACTER VARYING": 10485760, "CHARACTER": 10485760, "CHAR": 10485760,
    "DECIMAL": 1000, "NUMERIC": 1000,
}
_TYPE_PARAMS = re.compile(r"([A-Z ]+?)\((\d+)(?:, (\d+))?\)")


def _within_postgres_limits(normalized: str) -> bool:
    """Check length/precision/scale of a normalized type against PostgreSQL's limits."""
    match = _TYPE_PARAMS.match(normalized)
    if not match:
        return True
    base, first, second = match.group(1), int(match.group(2)), match.group(3)
    limit = _TYPE_LIMITS.get(base)
    if limit is not None and not 1 <= first <= limit:
        return False
    if second is not None:
        return base in ("DECIMAL", "NUMERIC") and int(second) <= first
    return True


class UnsafeDDLValueError(ValueError):
    """Raised when a value cannot be rendered safely into generated DDL."""


def quote_identifier(name: str) -> str:
    """Render a table/column name as a PostgreSQL identifier that cannot break out of its position.

    Plain names (letters, digits, underscore, non-leading $) keep their previous unquoted meaning:
    they are case-folded to lower case, as PostgreSQL does for unquoted
    identifiers, and are only double-quoted when they are reserved words.
    Any other name is emitted as a double-quoted identifier with embedded
    double quotes doubled, so it is always a single identifier token.
    """
    if not isinstance(name, str) or not name or "\x00" in name:
        raise UnsafeDDLValueError(f"Unsafe SQL identifier: {name!r}")
    if _PLAIN_IDENTIFIER.fullmatch(name):
        folded = name.lower()
        if folded not in POSTGRES_RESERVED_WORDS:
            return folded
        return f'"{folded}"'
    return '"' + name.replace('"', '""') + '"'


def safe_data_type(data_type: str | None, default: str = "VARCHAR(255)") -> str:
    """Return an allowlisted PostgreSQL column type (incl. INTERVAL, time zones, arrays), normalized."""
    if data_type is None:
        data_type = default
    if isinstance(data_type, str):
        if _DATA_TYPE.fullmatch(data_type.strip()):
            normalized = _normalize_type(data_type)
            if _within_postgres_limits(normalized):
                return normalized
    raise UnsafeDDLValueError(f"Unsupported SQL data type: {data_type!r}")


def sql_comment_text(value: object) -> str:
    """Flatten text for a ``--`` comment so line breaks cannot end the comment early."""
    return "".join(
        " " if unicodedata.category(ch) in ("Cc", "Cf", "Zl", "Zp") else ch
        for ch in str(value)
    )
