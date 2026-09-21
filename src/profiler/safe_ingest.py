"""Hardened file ingestion helpers for the source profiler.

Provides size/row/nesting limits and an XML parser that refuses document type
definitions, so entity-expansion attacks (billion laughs, quadratic blowup)
and oversized inputs cannot exhaust memory or CPU.
"""

import os
import re
import xml.etree.ElementTree as ET
from typing import Any, Optional

DEFAULT_MAX_FILE_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_ROWS = 1_000_000
DEFAULT_MAX_NESTING_DEPTH = 100

_PROLOG_SCAN_BYTES = 64 * 1024
_DOCTYPE_PATTERN = re.compile(rb"<!\s*DOCTYPE", re.IGNORECASE)


class IngestLimitError(ValueError):
    """Raised when an input file violates an ingestion safety limit."""


class IngestLimits:
    def __init__(
        self,
        max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
        max_rows: int = DEFAULT_MAX_ROWS,
        max_nesting_depth: int = DEFAULT_MAX_NESTING_DEPTH,
    ):
        self.max_file_bytes = max_file_bytes
        self.max_rows = max_rows
        self.max_nesting_depth = max_nesting_depth

    def check_file_size(self, file_path: str) -> None:
        size = os.path.getsize(file_path)
        if size > self.max_file_bytes:
            raise IngestLimitError(
                f"File {file_path} is {size} bytes, exceeding the {self.max_file_bytes} byte ingest limit"
            )

    def check_row_count(self, row_count: int, file_path: str) -> None:
        if row_count > self.max_rows:
            raise IngestLimitError(
                f"File {file_path} yields {row_count} rows, exceeding the {self.max_rows} row ingest limit"
            )

    def check_json_depth(self, value: Any, file_path: str) -> None:
        stack = [(value, 1)]
        while stack:
            node, depth = stack.pop()
            if depth > self.max_nesting_depth:
                raise IngestLimitError(
                    f"File {file_path} nests deeper than the {self.max_nesting_depth} level ingest limit"
                )
            if isinstance(node, dict):
                stack.extend((child, depth + 1) for child in node.values())
            elif isinstance(node, list):
                stack.extend((child, depth + 1) for child in node)

    def parse_xml(self, file_path: str) -> ET.Element:
        """Parse XML without DTD processing and with a bounded element depth."""
        self.check_file_size(file_path)
        parser = ET.XMLPullParser(events=("start", "end"))
        depth = 0
        root: Optional[ET.Element] = None

        with open(file_path, "rb") as handle:
            prolog = handle.read(_PROLOG_SCAN_BYTES)
            if _DOCTYPE_PATTERN.search(prolog):
                raise IngestLimitError(f"File {file_path} contains a DOCTYPE declaration, which is not allowed")
            chunk = prolog
            while chunk:
                parser.feed(chunk)
                for event, element in parser.read_events():
                    if event == "start":
                        depth += 1
                        if depth > self.max_nesting_depth:
                            raise IngestLimitError(
                                f"File {file_path} nests deeper than the "
                                f"{self.max_nesting_depth} level ingest limit"
                            )
                    else:
                        depth -= 1
                        if depth == 0:
                            root = element
                chunk = handle.read(_PROLOG_SCAN_BYTES)

        parser.close()
        for event, element in parser.read_events():
            if event == "end":
                depth -= 1
                if depth == 0:
                    root = element

        if root is None:
            raise IngestLimitError(f"File {file_path} contains no XML root element")
        return root
