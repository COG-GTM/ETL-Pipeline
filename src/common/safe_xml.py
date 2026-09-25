import os
from xml.etree.ElementTree import Element

from defusedxml.ElementTree import iterparse as _defused_iterparse

MAX_XML_BYTES = 64 * 1024 * 1024
MAX_XML_ELEMENTS = 500_000


def parse_xml_file(
    file_path: str,
    max_bytes: int = MAX_XML_BYTES,
    max_elements: int = MAX_XML_ELEMENTS,
) -> Element:
    """Parse an XML data feed with entity and size limits and return its root.

    Ingested feeds are third-party content, so parsing is delegated to
    defusedxml with DTDs, entity expansion and external references rejected,
    and the document is bounded by byte size and element count. Elements are
    counted as they are parsed so an oversized document is abandoned mid-parse
    instead of being materialised in full.
    """
    size = os.path.getsize(file_path)
    if size > max_bytes:
        raise ValueError(
            f"XML file exceeds size limit of {max_bytes} bytes: {file_path} ({size} bytes)"
        )

    root: Element | None = None
    element_count = 0
    for _event, element in _defused_iterparse(
        file_path,
        events=("start",),
        forbid_dtd=True,
        forbid_entities=True,
        forbid_external=True,
    ):
        if root is None:
            root = element
        element_count += 1
        if element_count > max_elements:
            raise ValueError(
                f"XML file exceeds element limit of {max_elements}: {file_path}"
            )

    if root is None:
        raise ValueError(f"XML file contains no elements: {file_path}")

    return root
