import os
from xml.etree.ElementTree import Element

from defusedxml.ElementTree import parse as _defused_parse

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
    and the document is bounded by byte size and element count.
    """
    size = os.path.getsize(file_path)
    if size > max_bytes:
        raise ValueError(
            f"XML file exceeds size limit of {max_bytes} bytes: {file_path} ({size} bytes)"
        )

    root = _defused_parse(
        file_path,
        forbid_dtd=True,
        forbid_entities=True,
        forbid_external=True,
    ).getroot()

    element_count = 0
    for _ in root.iter():
        element_count += 1
        if element_count > max_elements:
            raise ValueError(
                f"XML file exceeds element limit of {max_elements}: {file_path}"
            )

    return root
