import json
import os
import xml.etree.ElementTree as ET
import xml.parsers.expat
from datetime import datetime
from typing import Any

import pandas as pd

DEFAULT_MAX_FILE_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_ROWS = 5_000_000
DEFAULT_MAX_XML_DEPTH = 100
ALLOWED_JOIN_TYPES = frozenset({"left", "right", "inner", "outer"})


def _parse_xml_hardened(file_path: str, max_depth: int) -> ET.Element:
    """Parse an XML file with DTDs, entity declarations and external entity
    references rejected, and element nesting bounded.

    Closes off billion-laughs, quadratic blowup and XXE, which stdlib
    ``ElementTree.parse`` is documented to be vulnerable to.
    """

    def reject(message: str):
        def handler(*args: Any) -> None:
            raise ValueError(message)

        return handler

    builder = ET.TreeBuilder()
    depth = 0

    def start(tag: str, attrs: dict[str, str]) -> None:
        nonlocal depth
        depth += 1
        if depth > max_depth:
            raise ValueError(f"XML nesting exceeds the maximum depth of {max_depth}")
        builder.start(tag, attrs)

    def end(tag: str) -> None:
        nonlocal depth
        depth -= 1
        builder.end(tag)

    parser = xml.parsers.expat.ParserCreate()
    parser.StartDoctypeDeclHandler = reject("XML document type declarations are not allowed")
    parser.EntityDeclHandler = reject("XML entity declarations are not allowed")
    parser.UnparsedEntityDeclHandler = reject("XML entity declarations are not allowed")
    parser.ExternalEntityRefHandler = reject("External XML entity references are not allowed")
    parser.StartElementHandler = start
    parser.EndElementHandler = end
    parser.CharacterDataHandler = builder.data

    with open(file_path, "rb") as f:
        parser.ParseFile(f)

    return builder.close()


class DataConsolidator:
    def __init__(
        self,
        max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
        max_rows: int = DEFAULT_MAX_ROWS,
        max_xml_depth: int = DEFAULT_MAX_XML_DEPTH,
    ):
        self.sources: dict[str, pd.DataFrame] = {}
        self.consolidated: pd.DataFrame | None = None
        self.lineage: list[dict[str, Any]] = []
        self.max_file_bytes = max_file_bytes
        self.max_rows = max_rows
        self.max_xml_depth = max_xml_depth

    def load_source(self, name: str, file_path: str) -> pd.DataFrame:
        ext = os.path.splitext(file_path)[1].lower()
        self._check_file_size(file_path)

        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext == ".json":
            with open(file_path, "r") as f:
                raw = json.load(f)
            if isinstance(raw, list):
                df = pd.json_normalize(raw)
            elif isinstance(raw, dict):
                for key in ["data", "inventory", "items", "records", "results"]:
                    if key in raw and isinstance(raw[key], list):
                        df = pd.json_normalize(raw[key])
                        break
                else:
                    df = pd.json_normalize(raw)
            else:
                df = pd.DataFrame([raw])
        elif ext == ".xml":
            df = self._parse_xml(file_path)
        else:
            raise ValueError(f"Unsupported format: {ext}")

        if len(df) > self.max_rows:
            raise ValueError(
                f"Source '{name}' has {len(df)} rows, exceeding the limit of {self.max_rows}"
            )

        self.sources[name] = df
        self.lineage.append({
            "action": "load_source",
            "source_name": name,
            "file_path": file_path,
            "format": ext.replace(".", ""),
            "rows": len(df),
            "columns": list(df.columns),
            "timestamp": datetime.now().isoformat(),
        })
        return df

    def _check_file_size(self, file_path: str) -> None:
        size = os.path.getsize(file_path)
        if size > self.max_file_bytes:
            raise ValueError(
                f"Input file '{file_path}' is {size} bytes, exceeding the limit "
                f"of {self.max_file_bytes} bytes"
            )

    def _parse_xml(self, file_path: str) -> pd.DataFrame:
        root = _parse_xml_hardened(file_path, self.max_xml_depth)

        records = []
        for element in root.iter():
            if len(records) > self.max_rows:
                raise ValueError(
                    f"XML document yields more than {self.max_rows} records"
                )
            if len(element) > 0:
                record = {}
                has_leaf = False
                for child in element:
                    if child.text and child.text.strip():
                        record[child.tag] = child.text.strip()
                        has_leaf = True
                    elif len(child) > 0:
                        for subchild in child:
                            if subchild.text and subchild.text.strip():
                                record[f"{child.tag}_{subchild.tag}"] = subchild.text.strip()
                                has_leaf = True
                if has_leaf and len(record) >= 3:
                    records.append(record)

        return pd.DataFrame(records) if records else pd.DataFrame()

    def consolidate(
        self,
        primary_source: str,
        join_configs: list[dict[str, Any]],
    ) -> pd.DataFrame:
        if primary_source not in self.sources:
            raise ValueError(f"Primary source '{primary_source}' not loaded")

        result = self.sources[primary_source].copy()
        self.lineage.append({
            "action": "start_consolidation",
            "primary_source": primary_source,
            "primary_rows": len(result),
            "timestamp": datetime.now().isoformat(),
        })

        for config in join_configs:
            source_name = config["source"]
            join_key = config.get("on")
            left_key = config.get("left_on", join_key)
            right_key = config.get("right_on", join_key)
            how = config.get("how", "left")
            suffix = config.get("suffix", f"_{source_name}")

            if source_name not in self.sources:
                self.lineage.append({
                    "action": "skip_join",
                    "source": source_name,
                    "reason": "source not loaded",
                    "timestamp": datetime.now().isoformat(),
                })
                continue

            if how not in ALLOWED_JOIN_TYPES:
                raise ValueError(
                    f"Unsupported join type '{how}'; allowed: {sorted(ALLOWED_JOIN_TYPES)}"
                )

            right_df = self.sources[source_name]
            self._validate_join_keys(result, right_df, left_key, right_key, source_name)
            before_rows = len(result)
            cols_before = set(result.columns)

            estimated_rows = self._estimate_join_rows(result, right_df, left_key, right_key)
            if estimated_rows > self.max_rows:
                raise ValueError(
                    f"Join with '{source_name}' would produce about {estimated_rows} rows, "
                    f"exceeding the limit of {self.max_rows}"
                )

            result = result.merge(
                right_df,
                left_on=left_key,
                right_on=right_key,
                how=how,
                suffixes=("", suffix),
            )

            self.lineage.append({
                "action": "join",
                "source": source_name,
                "join_type": how,
                "join_keys": f"{left_key} = {right_key}",
                "rows_before": before_rows,
                "rows_after": len(result),
                "columns_added": [c for c in result.columns if c not in cols_before],
                "row_amplification": round(len(result) / before_rows, 2) if before_rows else None,
                "timestamp": datetime.now().isoformat(),
            })

        self.consolidated = result
        self.lineage.append({
            "action": "consolidation_complete",
            "final_rows": len(result),
            "final_columns": len(result.columns),
            "sources_merged": len(join_configs) + 1,
            "timestamp": datetime.now().isoformat(),
        })

        return result

    @staticmethod
    def _validate_join_keys(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_key: Any,
        right_key: Any,
        source_name: str,
    ) -> None:
        def as_list(key: Any, side: str) -> list[str]:
            if isinstance(key, str):
                return [key]
            if isinstance(key, (list, tuple)) and all(isinstance(k, str) for k in key):
                return list(key)
            raise ValueError(f"Invalid {side} join key for source '{source_name}': {key!r}")

        left_keys = as_list(left_key, "left")
        right_keys = as_list(right_key, "right")
        if len(left_keys) != len(right_keys):
            raise ValueError(f"Join key count mismatch for source '{source_name}'")

        missing_left = [k for k in left_keys if k not in left_df.columns]
        missing_right = [k for k in right_keys if k not in right_df.columns]
        if missing_left or missing_right:
            raise ValueError(
                f"Unknown join keys for source '{source_name}': "
                f"left={missing_left}, right={missing_right}"
            )

    @staticmethod
    def _estimate_join_rows(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_key: Any,
        right_key: Any,
    ) -> int:
        left_counts = left_df.groupby(left_key, dropna=False).size()
        right_counts = right_df.groupby(right_key, dropna=False).size()
        left_counts.index.names = right_counts.index.names
        matched = left_counts.mul(right_counts, fill_value=0)
        unmatched = max(len(left_df), len(right_df))
        return int(matched.sum()) + unmatched

    def get_consolidation_summary(self) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "sources_loaded": {},
            "consolidated_shape": None,
            "lineage": self.lineage,
        }

        for name, df in self.sources.items():
            summary["sources_loaded"][name] = {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
            }

        if self.consolidated is not None:
            summary["consolidated_shape"] = {
                "rows": len(self.consolidated),
                "columns": len(self.consolidated.columns),
                "column_names": list(self.consolidated.columns),
            }

        return summary

    def generate_report(self) -> str:
        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  DATA CONSOLIDATION REPORT")
        lines.append(f"{'='*70}")
        lines.append("")

        lines.append("  SOURCE SYSTEMS:")
        lines.append(f"  {'─'*50}")
        for name, df in self.sources.items():
            lines.append(f"  [{name}]")
            lines.append(f"    Rows: {len(df):,}  |  Columns: {len(df.columns)}")
            lines.append(f"    Columns: {', '.join(df.columns[:8])}{'...' if len(df.columns) > 8 else ''}")
            lines.append("")

        if self.consolidated is not None:
            lines.append("  CONSOLIDATED OUTPUT:")
            lines.append(f"  {'─'*50}")
            lines.append(f"  Total Rows: {len(self.consolidated):,}")
            lines.append(f"  Total Columns: {len(self.consolidated.columns)}")
            lines.append(f"  Columns: {', '.join(self.consolidated.columns[:10])}{'...' if len(self.consolidated.columns) > 10 else ''}")
            lines.append("")

        lines.append("  DATA LINEAGE:")
        lines.append(f"  {'─'*50}")
        for entry in self.lineage:
            action = entry.get("action", "unknown")
            ts = entry.get("timestamp", "")[:19]
            if action == "load_source":
                lines.append(f"  [{ts}] LOAD: {entry['source_name']} ({entry['format']}) - {entry['rows']} rows")
            elif action == "join":
                lines.append(f"  [{ts}] JOIN: {entry['source']} ({entry['join_type']}) on {entry['join_keys']}")
                lines.append(f"           Rows: {entry['rows_before']} -> {entry['rows_after']}")
            elif action == "consolidation_complete":
                lines.append(f"  [{ts}] DONE: {entry['final_rows']} rows, {entry['final_columns']} columns from {entry['sources_merged']} sources")

        return "\n".join(lines)
