import json
import os
import re
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

# Pseudo-null values that represent missing data but aren't actual NaN/None
PSEUDO_NULL_VALUES = {"", "N/A", "n/a", "NA", "null", "NULL", "Null", "none",
                      "None", "NONE", "-", "--", ".", "N/A ", " N/A", "nan",
                      "NaN", "NAN", "#N/A", "#NA", "missing", "MISSING"}


class SourceProfiler:
    def __init__(self) -> None:
        self.profile_results: dict[str, Any] = {}

    def profile_csv(self, file_path: str) -> dict[str, Any]:
        df = pd.read_csv(file_path)
        return self._profile_dataframe(df, file_path, "csv")

    def profile_json(self, file_path: str) -> dict[str, Any]:
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

        return self._profile_dataframe(df, file_path, "json")

    def profile_xml(self, file_path: str) -> dict[str, Any]:
        import xml.etree.ElementTree as ET

        tree = ET.parse(file_path)
        root = tree.getroot()

        records: list[dict[str, str]] = []
        for element in root.iter():
            if len(element) > 0 and any(child.text and child.text.strip() for child in element):
                record: dict[str, str] = {}
                for child in element:
                    if child.text and child.text.strip():
                        record[child.tag] = child.text.strip()
                    elif len(child) > 0:
                        for subchild in child:
                            if subchild.text and subchild.text.strip():
                                record[f"{child.tag}_{subchild.tag}"] = subchild.text.strip()
                if record:
                    records.append(record)

        df = pd.DataFrame(records) if records else pd.DataFrame()
        return self._profile_dataframe(df, file_path, "xml")

    def profile_auto(self, file_path: str) -> dict[str, Any]:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            return self.profile_csv(file_path)
        elif ext == ".json":
            return self.profile_json(file_path)
        elif ext == ".xml":
            return self.profile_xml(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    # ------------------------------------------------------------------
    # Core profiling logic
    # ------------------------------------------------------------------

    def _profile_dataframe(self, df: pd.DataFrame, source: str, format_type: str) -> dict[str, Any]:
        profile: dict[str, Any] = {
            "source": source,
            "format": format_type,
            "profiled_at": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {},
            "summary": {},
        }

        # Flatten complex nested types so downstream operations work on strings
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x, default=str) if isinstance(x, (list, dict)) else x)

        total_cells = len(df) * len(df.columns)
        total_nulls = int(df.isnull().sum().sum())
        try:
            total_duplicates = int(df.duplicated().sum())
        except TypeError:
            total_duplicates = 0

        memory_bytes = int(df.memory_usage(deep=True).sum())

        profile["summary"] = {
            "total_cells": total_cells,
            "total_null_cells": total_nulls,
            "null_percentage": round(total_nulls / total_cells * 100, 2) if total_cells > 0 else 0,
            "duplicate_rows": total_duplicates,
            "duplicate_percentage": round(total_duplicates / len(df) * 100, 2) if len(df) > 0 else 0,
            "memory_usage_bytes": memory_bytes,
            # --- Data Volume Metrics ---
            "estimated_memory_mb": round(memory_bytes / (1024 * 1024), 4),
        }

        for col in df.columns:
            col_profile = self._profile_column(df[col], len(df))
            profile["columns"][col] = col_profile

        self.profile_results[source] = profile
        return profile

    def _profile_column(self, series: pd.Series, total_rows: int) -> dict[str, Any]:
        col_info: dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_count": int(series.isnull().sum()),
            "null_percentage": round(series.isnull().sum() / len(series) * 100, 2) if len(series) > 0 else 0,
            "unique_count": int(series.nunique()),
            "unique_percentage": round(series.nunique() / len(series) * 100, 2) if len(series) > 0 else 0,
        }

        non_null = series.dropna()

        # --- Pseudo-null detection ---
        pseudo_null_count, pseudo_null_found = self._detect_pseudo_nulls(non_null)
        col_info["pseudo_null_count"] = pseudo_null_count
        if pseudo_null_found:
            col_info["pseudo_null_values"] = pseudo_null_found
        total_missing = col_info["null_count"] + pseudo_null_count
        col_info["total_missing"] = total_missing
        col_info["total_missing_percentage"] = (
            round(total_missing / len(series) * 100, 2) if len(series) > 0 else 0
        )

        # --- Cardinality flags ---
        col_info["is_potential_primary_key"] = (
            col_info["unique_count"] == len(series)
            and col_info["null_count"] == 0
            and len(series) > 0
        )
        col_info["is_categorical"] = (
            col_info["unique_count"] <= 20
            or (total_rows > 0 and col_info["unique_count"] / total_rows < 0.05)
        ) and not col_info["is_potential_primary_key"]

        # --- Mixed-type detection ---
        col_info["is_mixed_type"] = self._detect_mixed_types(non_null)

        if pd.api.types.is_numeric_dtype(series):
            col_info.update(self._profile_numeric_column(non_null))
        elif pd.api.types.is_string_dtype(series):
            col_info.update(self._profile_string_column(non_null, col_info))

        return col_info

    # ------------------------------------------------------------------
    # Numeric column profiling
    # ------------------------------------------------------------------

    def _profile_numeric_column(self, non_null: pd.Series) -> dict[str, Any]:
        """Profile a numeric column with stats, percentiles, and outlier detection."""
        result: dict[str, Any] = {}

        # Convert to float to handle boolean and integer dtypes uniformly
        numeric_vals = non_null.astype(float)

        stats: dict[str, Any] = {
            "mean": round(float(numeric_vals.mean()), 4) if len(numeric_vals) > 0 else None,
            "median": round(float(numeric_vals.median()), 4) if len(numeric_vals) > 0 else None,
            "std": round(float(numeric_vals.std()), 4) if len(numeric_vals) > 1 else None,
            "min": float(numeric_vals.min()) if len(numeric_vals) > 0 else None,
            "max": float(numeric_vals.max()) if len(numeric_vals) > 0 else None,
            "zeros": int((numeric_vals == 0).sum()),
            "negatives": int((numeric_vals < 0).sum()),
        }

        # --- Percentiles ---
        if len(numeric_vals) > 0:
            stats["p25"] = round(float(np.percentile(numeric_vals, 25)), 4)
            stats["p75"] = round(float(np.percentile(numeric_vals, 75)), 4)
            stats["p99"] = round(float(np.percentile(numeric_vals, 99)), 4)

        result["stats"] = stats

        # --- Outlier detection (IQR method) ---
        if len(numeric_vals) >= 4:
            q1 = float(np.percentile(numeric_vals, 25))
            q3 = float(np.percentile(numeric_vals, 75))
            iqr = q3 - q1
            lower_bound = round(q1 - 1.5 * iqr, 4)
            upper_bound = round(q3 + 1.5 * iqr, 4)
            outlier_mask = (numeric_vals < lower_bound) | (numeric_vals > upper_bound)
            result["outliers"] = {
                "method": "IQR",
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "outlier_count": int(outlier_mask.sum()),
            }

        return result

    # ------------------------------------------------------------------
    # String column profiling
    # ------------------------------------------------------------------

    def _profile_string_column(self, non_null: pd.Series, col_info: dict[str, Any]) -> dict[str, Any]:
        """Profile a string column with length stats, role inference, semantic/pattern detection."""
        result: dict[str, Any] = {}
        str_values = non_null.astype(str)

        lengths = str_values.str.len()
        result["stats"] = {
            "min_length": int(lengths.min()) if len(lengths) > 0 else None,
            "max_length": int(lengths.max()) if len(lengths) > 0 else None,
            "avg_length": round(float(lengths.mean()), 2) if len(lengths) > 0 else None,
        }

        # Inferred role (existing logic preserved)
        if col_info["unique_percentage"] > 90:
            result["inferred_role"] = "identifier"
        elif col_info["unique_count"] <= 20:
            result["inferred_role"] = "categorical"
        else:
            result["inferred_role"] = "text"

        # --- Top values for ALL string columns (not just categorical) ---
        result["top_values"] = non_null.value_counts().head(10).to_dict()

        # Semantic type detection (existing: email, phone, date + new: uuid, url, zip_code, currency)
        semantic_type = self._detect_semantic_type(str_values, non_null, result.get("inferred_role", ""))
        if semantic_type:
            result["semantic_type"] = semantic_type

        # --- Has time component for date columns ---
        if result.get("semantic_type") == "date":
            result["has_time_component"] = self._detect_time_component(non_null)

        return result

    # ------------------------------------------------------------------
    # Semantic type detection
    # ------------------------------------------------------------------

    def _detect_semantic_type(self, str_values: pd.Series, non_null: pd.Series, inferred_role: str) -> str | None:
        """Detect the semantic type of a string column."""
        if len(non_null) == 0:
            return None

        threshold = 0.8
        count = len(non_null)

        # Email
        email_match = str_values.str.match(r"^[\w.+-]+@[\w-]+\.[\w.]+$")
        if email_match.sum() > count * threshold:
            return "email"

        # Phone
        phone_match = str_values.str.match(r"^[\d\s\-\(\)\+]+$")
        if phone_match.sum() > count * threshold and inferred_role != "identifier":
            return "phone"

        # UUID
        uuid_match = str_values.str.match(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        )
        if uuid_match.sum() > count * threshold:
            return "uuid"

        # URL
        url_match = str_values.str.match(r"^https?://[^\s]+$")
        if url_match.sum() > count * threshold:
            return "url"

        # ZIP code (US 5-digit or 5+4)
        zip_match = str_values.str.match(r"^\d{5}(-\d{4})?$")
        if zip_match.sum() > count * threshold:
            return "zip_code"

        # Currency values (e.g., $1,234.56)
        currency_match = str_values.str.match(r"^[$\u20ac\u00a3\u00a5]-?\d{1,3}(,\d{3})*(\.\d{1,2})?$")
        if currency_match.sum() > count * threshold:
            return "currency"

        # Date (sample-based for performance)
        date_parseable = 0
        for val in non_null.head(20):
            try:
                pd.to_datetime(val)
                date_parseable += 1
            except (ValueError, TypeError):
                pass
        sample_size = min(20, len(non_null))
        if sample_size > 0 and date_parseable / sample_size > threshold:
            return "date"

        return None

    # ------------------------------------------------------------------
    # Helper: pseudo-null detection
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_pseudo_nulls(series: pd.Series) -> tuple[int, list[str]]:
        """Count values that look like nulls (empty strings, 'N/A', 'null', etc.)."""
        if len(series) == 0:
            return 0, []
        str_vals = series.astype(str).str.strip()
        mask = str_vals.isin(PSEUDO_NULL_VALUES)
        count = int(mask.sum())
        found_values = sorted(str_vals[mask].unique().tolist()) if count > 0 else []
        return count, found_values

    # ------------------------------------------------------------------
    # Helper: mixed-type detection
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_mixed_types(series: pd.Series) -> bool:
        """Detect whether a column has mixed types (e.g., mostly numeric with some strings)."""
        if len(series) == 0:
            return False
        # For object-dtype columns, check if some values are numeric and some are not
        if series.dtype == object:
            numeric_count = 0
            non_numeric_count = 0
            for val in series:
                str_val = str(val).strip()
                if str_val in PSEUDO_NULL_VALUES:
                    continue
                try:
                    float(str_val)
                    numeric_count += 1
                except (ValueError, TypeError):
                    non_numeric_count += 1
            # Mixed if both numeric and non-numeric are present in significant portion
            total = numeric_count + non_numeric_count
            if total > 0:
                numeric_ratio = numeric_count / total
                return 0.05 < numeric_ratio < 0.95
        return False

    # ------------------------------------------------------------------
    # Helper: date-time component detection
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_time_component(series: pd.Series) -> bool:
        """Check if date-like values contain a time component (HH:MM, T, etc.)."""
        sample = series.head(20).astype(str)
        time_pattern = re.compile(r"[T ]\d{2}:\d{2}")
        matches = sum(1 for val in sample if time_pattern.search(val))
        return matches > len(sample) * 0.5

    # ------------------------------------------------------------------
    # Cross-source analysis
    # ------------------------------------------------------------------

    def profile_cross_source(self, profiles: list[dict[str, Any]]) -> dict[str, Any]:
        """Analyze relationships across multiple profiled sources.

        Returns a dict with:
        - shared_columns: columns that appear in multiple sources
        - potential_foreign_keys: columns in one source that may reference a PK in another
        """
        # Build a map: column_name -> list of source names
        column_sources: dict[str, list[str]] = {}
        # Build a map: source -> { col_name: col_info }
        source_columns: dict[str, dict[str, Any]] = {}

        for profile in profiles:
            src_name = os.path.basename(profile["source"])
            source_columns[src_name] = profile.get("columns", {})
            for col_name in profile.get("columns", {}):
                column_sources.setdefault(col_name, []).append(src_name)

        # Shared columns: appear in 2+ sources
        shared_columns: dict[str, list[str]] = {
            col: sources for col, sources in column_sources.items() if len(sources) > 1
        }

        # Potential foreign key relationships
        # Find PK-like columns (100% unique, no nulls) and see if they appear elsewhere
        pk_columns: dict[str, str] = {}  # col_name -> source_name (where it's a PK)
        for profile in profiles:
            src_name = os.path.basename(profile["source"])
            for col_name, col_info in profile.get("columns", {}).items():
                if col_info.get("is_potential_primary_key", False):
                    pk_columns[col_name] = src_name

        potential_foreign_keys: list[dict[str, str]] = []
        for col_name, pk_source in pk_columns.items():
            for other_source, cols in source_columns.items():
                if other_source != pk_source and col_name in cols:
                    potential_foreign_keys.append({
                        "column": col_name,
                        "primary_key_source": pk_source,
                        "foreign_key_source": other_source,
                    })

        return {
            "shared_columns": shared_columns,
            "potential_foreign_keys": potential_foreign_keys,
        }

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------

    def generate_report(self, profile: dict[str, Any]) -> str:
        lines: list[str] = []
        lines.append(f"{'='*70}")
        lines.append("  SOURCE DATA PROFILE REPORT")
        lines.append(f"{'='*70}")
        lines.append(f"  Source: {profile['source']}")
        lines.append(f"  Format: {profile['format'].upper()}")
        lines.append(f"  Profiled: {profile['profiled_at']}")
        lines.append(f"{'='*70}")
        lines.append("")

        summary = profile["summary"]
        lines.append("  DATASET OVERVIEW")
        lines.append(f"  {'-'*40}")
        lines.append(f"  Rows:            {profile['row_count']:,}")
        lines.append(f"  Columns:         {profile['column_count']}")
        lines.append(f"  Total Cells:     {summary['total_cells']:,}")
        lines.append(f"  Null Cells:      {summary['total_null_cells']:,} ({summary['null_percentage']}%)")
        lines.append(f"  Duplicate Rows:  {summary['duplicate_rows']} ({summary['duplicate_percentage']}%)")
        lines.append(f"  Memory Usage:    {summary['memory_usage_bytes']:,} bytes ({summary.get('estimated_memory_mb', 0)} MB)")
        lines.append("")

        lines.append("  COLUMN DETAILS")
        lines.append(f"  {'-'*40}")

        for col_name, col_info in profile["columns"].items():
            lines.append(f"  [{col_name}]")
            lines.append(f"    Type: {col_info['dtype']}")
            lines.append(f"    Nulls: {col_info['null_count']} ({col_info['null_percentage']}%)")
            lines.append(f"    Unique: {col_info['unique_count']} ({col_info['unique_percentage']}%)")

            # Pseudo-null info
            pseudo = col_info.get("pseudo_null_count", 0)
            if pseudo > 0:
                lines.append(f"    Pseudo-Nulls: {pseudo} (values: {col_info.get('pseudo_null_values', [])})")
            total_missing = col_info.get("total_missing", col_info["null_count"])
            total_missing_pct = col_info.get("total_missing_percentage", col_info["null_percentage"])
            if total_missing > col_info["null_count"]:
                lines.append(f"    Total Missing (incl. pseudo): {total_missing} ({total_missing_pct}%)")

            # Cardinality flags
            if col_info.get("is_potential_primary_key"):
                lines.append("    ** Potential Primary Key **")
            if col_info.get("is_categorical"):
                lines.append("    Flag: Categorical (low cardinality)")

            # Mixed type warning
            if col_info.get("is_mixed_type"):
                lines.append("    WARNING: Mixed types detected in column")

            if "semantic_type" in col_info:
                sem_label = col_info["semantic_type"]
                if col_info.get("has_time_component"):
                    sem_label += " (with time)"
                lines.append(f"    Semantic Type: {sem_label}")
            if "inferred_role" in col_info:
                lines.append(f"    Inferred Role: {col_info['inferred_role']}")

            if "stats" in col_info:
                stats = col_info["stats"]
                for k, v in stats.items():
                    if v is not None and k not in ("zeros", "negatives"):
                        lines.append(f"    {k}: {v}")
                if stats.get("negatives", 0) > 0:
                    lines.append(f"    WARNING: {stats['negatives']} negative values detected")

            # Outlier info (numeric columns)
            if "outliers" in col_info:
                o = col_info["outliers"]
                lines.append(f"    Outliers ({o['method']}): {o['outlier_count']} values outside [{o['lower_bound']}, {o['upper_bound']}]")

            if "top_values" in col_info:
                lines.append(f"    Top Values: {dict(list(col_info['top_values'].items())[:5])}")
            lines.append("")

        return "\n".join(lines)

    def generate_cross_source_report(self, cross_analysis: dict[str, Any]) -> str:
        """Generate a human-readable report for cross-source analysis."""
        lines: list[str] = []
        lines.append(f"{'='*70}")
        lines.append("  CROSS-SOURCE ANALYSIS REPORT")
        lines.append(f"{'='*70}")
        lines.append("")

        shared = cross_analysis.get("shared_columns", {})
        if shared:
            lines.append("  SHARED COLUMNS (identical names across sources)")
            lines.append(f"  {'-'*50}")
            for col_name, sources in shared.items():
                lines.append(f"    {col_name}: found in {', '.join(sources)}")
            lines.append("")
        else:
            lines.append("  No shared column names detected across sources.")
            lines.append("")

        fk_candidates = cross_analysis.get("potential_foreign_keys", [])
        if fk_candidates:
            lines.append("  POTENTIAL FOREIGN KEY RELATIONSHIPS")
            lines.append(f"  {'-'*50}")
            for fk in fk_candidates:
                lines.append(
                    f"    {fk['column']}: PK in [{fk['primary_key_source']}] "
                    f"-> FK in [{fk['foreign_key_source']}]"
                )
            lines.append("")
        else:
            lines.append("  No potential foreign key relationships detected.")
            lines.append("")

        return "\n".join(lines)
