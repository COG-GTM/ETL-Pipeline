from datetime import datetime
from typing import Any

from src.sql_identifiers import quote_identifier, truncate_identifier, unique_identifier


class TargetModelDesigner:
    DIMENSION_AUDIT_COLUMNS = ("effective_date", "expiry_date", "is_current")
    FACT_AUDIT_COLUMNS = ("etl_load_timestamp", "etl_batch_id")

    def __init__(self):
        self.source_schemas: list[dict[str, Any]] = []
        self.target_model: dict[str, Any] = {}

    def add_source_schema(self, schema: dict[str, Any]) -> None:
        self.source_schemas.append(schema)

    def design_star_schema(self, fact_table_hint: str = "transactions") -> dict[str, Any]:
        all_columns: dict[str, list[dict[str, Any]]] = {}
        used_table_names: set[str] = set()
        for schema in self.source_schemas:
            if not schema["columns"]:
                continue
            table_name = unique_identifier(schema["detected_table_name"], used_table_names)
            all_columns[table_name] = list(schema["columns"])

        fact_table = None
        dimension_tables = []

        for table_name, columns in all_columns.items():
            if fact_table is None and fact_table_hint.lower() in table_name.lower():
                fact_table = {"name": truncate_identifier(f"fact_{table_name}"), "source": table_name, "columns": columns}
            else:
                dimension_tables.append({"name": truncate_identifier(f"dim_{table_name}"), "source": table_name, "columns": columns})

        if fact_table is None and all_columns:
            max_table = max(all_columns.items(), key=lambda x: len(x[1]))
            fact_table = {"name": truncate_identifier(f"fact_{max_table[0]}"), "source": max_table[0], "columns": max_table[1]}
            dimension_tables = [
                {"name": truncate_identifier(f"dim_{t}"), "source": t, "columns": c}
                for t, c in all_columns.items()
                if t != max_table[0]
            ]

        if fact_table is not None:
            fact_table["columns"] = self._avoid_reserved_names(fact_table["columns"], self.FACT_AUDIT_COLUMNS, fact_table["name"])
        for dim in dimension_tables:
            dim["columns"] = self._avoid_reserved_names(dim["columns"], self.DIMENSION_AUDIT_COLUMNS, dim["name"])

        relationships = self._infer_relationships(fact_table, dimension_tables)

        self.target_model = {
            "designed_at": datetime.now().isoformat(),
            "schema_type": "star_schema",
            "fact_table": fact_table,
            "dimension_tables": dimension_tables,
            "relationships": relationships,
            "aggregation_tables": self._design_aggregations(fact_table),
        }

        return self.target_model

    def _infer_relationships(self, fact_table: dict[str, Any] | None, dim_tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if fact_table is None:
            return []

        relationships = []
        fact_col_names = {c["name"] for c in fact_table["columns"]}

        for dim in dim_tables:
            dim_col_names = {c["name"] for c in dim["columns"]}

            shared = fact_col_names & dim_col_names
            for col_name in shared:
                if col_name.endswith("_id") or col_name.endswith("_key") or col_name == "id":
                    relationships.append({
                        "type": "foreign_key",
                        "from_table": fact_table["name"],
                        "from_column": col_name,
                        "to_table": dim["name"],
                        "to_column": col_name,
                        "cardinality": "many_to_one",
                    })

            dim_source_lower = dim["source"].lower()
            for fc in fact_col_names:
                if fc.endswith("_id") and dim_source_lower.startswith(fc.replace("_id", "")):
                    if fc not in shared:
                        relationships.append({
                            "type": "inferred_foreign_key",
                            "from_table": fact_table["name"],
                            "from_column": fc,
                            "to_table": dim["name"],
                            "to_column": next(
                                (c["name"] for c in dim["columns"] if c.get("is_unique")),
                                f"{dim_source_lower}_id",
                            ),
                            "cardinality": "many_to_one",
                        })

        return relationships

    def _design_aggregations(self, fact_table: dict[str, Any] | None) -> list[dict[str, Any]]:
        if fact_table is None:
            return []

        agg_tables = []
        numeric_cols = [c["name"] for c in fact_table["columns"] if "DECIMAL" in c.get("target_dtype", "") or "INTEGER" in c.get("target_dtype", "")]
        date_cols = [c["name"] for c in fact_table["columns"] if "DATE" in c.get("target_dtype", "") or "TIMESTAMP" in c.get("target_dtype", "")]
        id_cols = [c["name"] for c in fact_table["columns"] if c["name"].endswith("_id")]

        if numeric_cols and date_cols:
            agg_tables.append({
                "name": f"agg_{fact_table['source']}_daily",
                "grain": "daily",
                "group_by": date_cols[:1] + id_cols[:2],
                "measures": [{"column": nc, "aggregations": ["SUM", "AVG", "COUNT", "MIN", "MAX"]} for nc in numeric_cols[:3]],
            })

        if numeric_cols and id_cols:
            agg_tables.append({
                "name": f"agg_{fact_table['source']}_by_entity",
                "grain": "entity",
                "group_by": id_cols[:2],
                "measures": [{"column": nc, "aggregations": ["SUM", "AVG", "COUNT"]} for nc in numeric_cols[:3]],
            })

        return agg_tables

    def generate_target_ddl(self) -> str:
        if not self.target_model:
            return "-- No target model designed yet"

        lines = ["-- Auto-generated Target Data Model DDL"]
        lines.append(f"-- Designed: {self.target_model['designed_at']}")
        lines.append(f"-- Schema Type: {self.target_model['schema_type']}")
        lines.append("")

        for dim in self.target_model.get("dimension_tables", []):
            lines.append(f"CREATE TABLE {quote_identifier(dim['name'])} (")
            col_defs = []
            col_defs.append(f"    {self._surrogate_key(dim['name'])} SERIAL PRIMARY KEY")
            for col in dim["columns"]:
                col_defs.append(self._column_def(col))
            col_defs.append("    effective_date DATE DEFAULT CURRENT_DATE")
            col_defs.append("    expiry_date DATE DEFAULT '9999-12-31'")
            col_defs.append("    is_current BOOLEAN DEFAULT TRUE")
            lines.append(",\n".join(col_defs))
            lines.append(");\n")

        fact = self.target_model.get("fact_table")
        if fact:
            lines.append(f"CREATE TABLE {quote_identifier(fact['name'])} (")
            col_defs = [f"    {self._surrogate_key(fact['name'])} SERIAL PRIMARY KEY"]
            for col in fact["columns"]:
                col_defs.append(self._column_def(col))
            col_defs.append("    etl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            col_defs.append("    etl_batch_id VARCHAR(50)")
            lines.append(",\n".join(col_defs))
            lines.append(");\n")

        for rel in self.target_model.get("relationships", []):
            lines.append(f"-- Relationship: {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']} ({rel['cardinality']})")

        for agg in self.target_model.get("aggregation_tables", []):
            lines.append(f"\n-- Aggregation Table: {agg['name']} (grain: {agg['grain']})")

        return "\n".join(lines)

    @staticmethod
    def _column_def(col: dict[str, Any]) -> str:
        not_null = "" if col.get("nullable", True) else " NOT NULL"
        return f"    {quote_identifier(col['name'])} {col.get('target_dtype', 'VARCHAR(255)')}{not_null}"

    @staticmethod
    def _surrogate_key_name(table_name: str) -> str:
        return truncate_identifier(f"{table_name}_sk")

    def _surrogate_key(self, table_name: str) -> str:
        return quote_identifier(self._surrogate_key_name(table_name))

    def _avoid_reserved_names(self, columns: list[dict[str, Any]], audit_columns: tuple, table_name: str) -> list[dict[str, Any]]:
        used = {self._surrogate_key_name(table_name), *audit_columns}
        renamed = []
        for col in columns:
            name = unique_identifier(col["name"], used)
            renamed.append(col if name == col["name"] else {**col, "name": name})
        return renamed

    def generate_report(self) -> str:
        if not self.target_model:
            return "No target model designed yet."

        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  TARGET DATA MODEL DESIGN")
        lines.append(f"{'='*70}")
        lines.append(f"  Schema Type: {self.target_model['schema_type']}")
        lines.append(f"  Designed: {self.target_model['designed_at']}")
        lines.append("")

        fact = self.target_model.get("fact_table")
        if fact:
            lines.append(f"  FACT TABLE: {fact['name']}")
            lines.append(f"  Source: {fact['source']}")
            lines.append(f"  Columns: {len(fact['columns'])}")
            for col in fact["columns"]:
                lines.append(f"    - {col['name']:30s} {col.get('target_dtype', 'N/A')}")
            lines.append("")

        dims = self.target_model.get("dimension_tables", [])
        if dims:
            lines.append(f"  DIMENSION TABLES ({len(dims)}):")
            for dim in dims:
                lines.append(f"  {dim['name']} (from {dim['source']}, {len(dim['columns'])} cols)")
                for col in dim["columns"]:
                    lines.append(f"    - {col['name']:30s} {col.get('target_dtype', 'N/A')}")
                lines.append("")

        rels = self.target_model.get("relationships", [])
        if rels:
            lines.append(f"  RELATIONSHIPS ({len(rels)}):")
            for r in rels:
                lines.append(f"    {r['from_table']}.{r['from_column']} -> {r['to_table']}.{r['to_column']} [{r['cardinality']}]")
            lines.append("")

        aggs = self.target_model.get("aggregation_tables", [])
        if aggs:
            lines.append(f"  AGGREGATION TABLES ({len(aggs)}):")
            for a in aggs:
                lines.append(f"    {a['name']} (grain: {a['grain']})")
                lines.append(f"      Group By: {', '.join(a['group_by'])}")
                for m in a["measures"]:
                    lines.append(f"      {m['column']}: {', '.join(m['aggregations'])}")

        return "\n".join(lines)
