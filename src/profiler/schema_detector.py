import hashlib
import os
import re
from typing import Any

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENTIFIER_LEN = 63


def quote_identifier(name: str) -> str:
    """Validate a SQL identifier against a strict allowlist and double-quote it."""
    if not isinstance(name, str) or not _IDENTIFIER_RE.fullmatch(name) or len(name) > _MAX_IDENTIFIER_LEN:
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


def sanitize_identifier(raw: str, fallback: str = "unnamed") -> str:
    """Normalize an externally-supplied name into a safe, lowercase SQL identifier."""
    name = re.sub(r"[^a-z0-9_]", "_", str(raw).lower())
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = fallback
    elif not re.match(r"[a-z_]", name):
        name = f"_{name}"
    return _truncate_identifier(name)


def _truncate_identifier(name: str) -> str:
    """Shorten an identifier to the max length, keeping it unique via a hash suffix."""
    if len(name) <= _MAX_IDENTIFIER_LEN:
        return name
    digest = hashlib.sha1(name.encode()).hexdigest()[:8]
    return f"{name[:_MAX_IDENTIFIER_LEN - len(digest) - 1]}_{digest}"


class SchemaDetector:
    TYPE_MAP = {
        "int64": "INTEGER",
        "float64": "DECIMAL",
        "object": "VARCHAR",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
    }

    def detect_schema(self, profile: dict[str, Any]) -> dict[str, Any]:
        schema: dict[str, Any] = {
            "source": profile["source"],
            "detected_table_name": self._infer_table_name(profile["source"]),
            "columns": [],
            "primary_key_candidates": [],
            "foreign_key_candidates": [],
            "indexes_recommended": [],
        }

        used_names: set[str] = set()
        for source_name, col_info in profile["columns"].items():
            col_name = sanitize_identifier(source_name, fallback="column")
            if col_name in used_names:
                suffix = 2
                while True:
                    candidate = _truncate_identifier(f"{col_name}_{suffix}")
                    if candidate not in used_names:
                        col_name = candidate
                        break
                    suffix += 1
            used_names.add(col_name)

            col_schema = {
                "name": col_name,
                "source_name": source_name,
                "source_dtype": col_info["dtype"],
                "target_dtype": self._map_dtype(col_info),
                "nullable": col_info["null_count"] > 0,
                "is_unique": col_info["unique_percentage"] == 100.0,
            }

            if col_info.get("semantic_type") == "email":
                col_schema["target_dtype"] = "VARCHAR(255)"
                col_schema["validation"] = "email_format"
            elif col_info.get("semantic_type") == "phone":
                col_schema["target_dtype"] = "VARCHAR(20)"
                col_schema["validation"] = "phone_format"
            elif col_info.get("semantic_type") == "date":
                col_schema["target_dtype"] = "DATE"

            if col_info.get("inferred_role") == "identifier" and col_info["null_count"] == 0:
                schema["primary_key_candidates"].append(col_name)
            elif col_name.endswith("_id") or col_name.endswith("_key"):
                schema["foreign_key_candidates"].append(col_name)

            if col_info.get("inferred_role") == "categorical":
                schema["indexes_recommended"].append(col_name)

            schema["columns"].append(col_schema)

        return schema

    def _infer_table_name(self, source: str) -> str:
        base = os.path.splitext(os.path.basename(source))[0]
        return sanitize_identifier(base, fallback="t_unnamed")

    def _map_dtype(self, col_info: dict[str, Any]) -> str:
        dtype = col_info["dtype"]
        if dtype in self.TYPE_MAP:
            mapped = self.TYPE_MAP[dtype]
            if mapped == "VARCHAR" and "stats" in col_info:
                max_len = col_info["stats"].get("max_length", 255)
                if max_len:
                    return f"VARCHAR({min(int(max_len * 1.5), 4000)})"
            return mapped
        return "VARCHAR(255)"

    def generate_ddl(self, schema: dict[str, Any]) -> str:
        lines = []
        raw_table_name = schema["detected_table_name"]
        table_name = quote_identifier(raw_table_name)
        lines.append(f"CREATE TABLE {table_name} (")

        col_defs = []
        for col in schema["columns"]:
            parts = [f"    {quote_identifier(col['name'])}"]
            parts.append(col["target_dtype"])
            if not col["nullable"]:
                parts.append("NOT NULL")
            if col["is_unique"] and col["name"] not in schema.get("primary_key_candidates", []):
                parts.append("UNIQUE")
            col_defs.append(" ".join(parts))

        if schema["primary_key_candidates"]:
            pk_cols = ", ".join(quote_identifier(c) for c in schema["primary_key_candidates"][:1])
            col_defs.append(f"    PRIMARY KEY ({pk_cols})")

        lines.append(",\n".join(col_defs))
        lines.append(");")

        for idx_col in schema.get("indexes_recommended", []):
            idx_name = quote_identifier(_truncate_identifier(f"idx_{raw_table_name}_{idx_col}"))
            lines.append(f"CREATE INDEX {idx_name} ON {table_name} ({quote_identifier(idx_col)});")

        return "\n".join(lines)

    def generate_schema_report(self, schema: dict[str, Any]) -> str:
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"  DETECTED SCHEMA: {schema['detected_table_name']}")
        lines.append(f"{'='*60}")
        lines.append(f"  Source: {schema['source']}")
        lines.append(f"  Columns: {len(schema['columns'])}")
        lines.append("")

        for col in schema["columns"]:
            nullable = "NULL" if col["nullable"] else "NOT NULL"
            unique = " UNIQUE" if col["is_unique"] else ""
            lines.append(f"  {col['name']:30s} {col['target_dtype']:20s} {nullable}{unique}")

        if schema["primary_key_candidates"]:
            lines.append(f"\n  Primary Key Candidates: {', '.join(schema['primary_key_candidates'])}")
        if schema["foreign_key_candidates"]:
            lines.append(f"  Foreign Key Candidates: {', '.join(schema['foreign_key_candidates'])}")
        if schema["indexes_recommended"]:
            lines.append(f"  Recommended Indexes: {', '.join(schema['indexes_recommended'])}")

        return "\n".join(lines)
