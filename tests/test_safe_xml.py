import pytest

from src.common.safe_xml import parse_xml_file
from src.consolidator.data_consolidator import DataConsolidator
from src.profiler.source_profiler import SourceProfiler

BILLION_LAUGHS = """<?xml version="1.0"?>
<!DOCTYPE inventory [
  <!ENTITY lol "lol">
  <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
  <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">
  <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
  <!ENTITY lol4 "&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;">
]>
<inventory>
  <vehicle>
    <vin>&lol4;</vin>
    <make>Acme</make>
    <model>Roadster</model>
  </vehicle>
</inventory>
"""

VALID_FEED = """<?xml version="1.0"?>
<inventory>
  <vehicle>
    <vin>1HGCM82633A004352</vin>
    <make>Honda</make>
    <model>Accord</model>
  </vehicle>
  <vehicle>
    <vin>1HGCM82633A004353</vin>
    <make>Toyota</make>
    <model>Camry</model>
  </vehicle>
</inventory>
"""


def _write(tmp_path, content):
    path = tmp_path / "feed.xml"
    path.write_text(content)
    return str(path)


def test_valid_feed_is_parsed(tmp_path):
    df = DataConsolidator().load_source("inventory", _write(tmp_path, VALID_FEED))

    assert not df.empty
    assert {"vin", "make", "model"}.issubset(df.columns)
    assert set(df["vin"].dropna()) == {"1HGCM82633A004352", "1HGCM82633A004353"}


def test_profiler_parses_valid_feed(tmp_path):
    profile = SourceProfiler().profile_xml(_write(tmp_path, VALID_FEED))

    assert profile["row_count"] > 0


def test_entity_expansion_is_rejected(tmp_path):
    with pytest.raises(Exception) as exc_info:
        DataConsolidator().load_source("inventory", _write(tmp_path, BILLION_LAUGHS))

    assert "dtd" in type(exc_info.value).__name__.lower() or "Dtd" in str(exc_info.value)


def test_entity_expansion_is_rejected_by_profiler(tmp_path):
    with pytest.raises(Exception) as exc_info:
        SourceProfiler().profile_xml(_write(tmp_path, BILLION_LAUGHS))

    assert "dtd" in type(exc_info.value).__name__.lower() or "Dtd" in str(exc_info.value)


def test_oversized_file_is_rejected(tmp_path):
    path = _write(tmp_path, VALID_FEED)

    with pytest.raises(ValueError, match="size limit"):
        parse_xml_file(path, max_bytes=10)


def test_element_count_limit_is_enforced(tmp_path):
    path = _write(tmp_path, VALID_FEED)

    with pytest.raises(ValueError, match="element limit"):
        parse_xml_file(path, max_elements=3)
