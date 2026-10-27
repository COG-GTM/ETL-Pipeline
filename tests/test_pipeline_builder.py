"""Tests for PipelineBuilder.generate_pipeline_code (pytest style).

Runnable with ``pytest tests/`` or, without pytest installed,
``python -m tests.test_pipeline_builder``.
"""
import ast
import os
import sys
import tempfile
import types

import pandas as pd

from src.pipeline_generator import PipelineBuilder
from src.profiler.source_profiler import SourceProfiler

INJECTED_PATH = 'x"); raise SystemExit("pwned"); ("y.csv'
READERS = {"csv": "read_csv", "json": "read_json"}


def _generate(sources):
    """Build a pipeline for ``sources``; return (builder, generated code)."""
    builder = PipelineBuilder()
    builder.generate_pipeline_from_sources(sources, [], {})
    return builder, builder.generate_pipeline_code()


def _method(tree, prefix):
    """Return the single GeneratedPipeline method starting with prefix."""
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
    matches = [
        n for n in cls.body
        if isinstance(n, ast.FunctionDef) and n.name.startswith(prefix)
    ]
    assert len(matches) == 1, [n.name for n in matches]
    return matches[0]


def _calls(node, attr):
    """Return every ``<obj>.<attr>(...)`` call under ``node``."""
    return [
        n for n in ast.walk(node)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == attr
    ]


def _has_raise(tree):
    """Return True if the parsed code contains a ``raise`` statement."""
    return any(isinstance(n, ast.Raise) for n in ast.walk(tree))


def _run_with_stub_pandas(code):
    """Execute generated code against a pandas stub; return its reads."""
    reads = []
    stub = types.ModuleType("pandas")
    stub.read_csv = lambda path: reads.append(("csv", path))
    stub.read_json = lambda path: reads.append(("json", path))
    real = sys.modules.get("pandas")
    sys.modules["pandas"] = stub
    try:
        namespace = {}
        exec(compile(code, "<generated>", "exec"), namespace)
        namespace["GeneratedPipeline"]().run()
    finally:
        sys.modules["pandas"] = real
    return reads


def test_quote_in_source_path_stays_a_string_constant():
    """A quote in the path cannot close the read_csv/read_json literal."""
    for fmt, reader in READERS.items():
        source = {"source": INJECTED_PATH, "format": fmt, "row_count": 1}
        _, code = _generate([source])
        tree = ast.parse(code)
        calls = _calls(_method(tree, "_extract_"), reader)
        assert len(calls) == 1
        assert len(calls[0].args) == 1
        assert isinstance(calls[0].args[0], ast.Constant)
        assert calls[0].args[0].value == INJECTED_PATH
        assert not _has_raise(tree)


def test_generated_code_reads_exact_path_without_running_payload():
    """Executing the generated code reads the path and runs nothing else."""
    source = {"source": INJECTED_PATH, "format": "csv", "row_count": 1}
    _, code = _generate([source])
    assert _run_with_stub_pandas(code) == [("csv", INJECTED_PATH)]


def test_newline_in_comment_values_cannot_start_code():
    """Newlines in comment/docstring values cannot inject statements."""
    source = "feed.xml\nraise SystemExit('pwned')\r__import__('os')"
    builder, _ = _generate(
        [{"source": source, "format": "xml\nimport os", "row_count": 1}]
    )
    builder.pipeline_config["generated_at"] = '"""\nraise SystemExit(1)\n"""'
    code = builder.generate_pipeline_code()
    tree = ast.parse(code)
    assert not _has_raise(tree)
    imports = [
        a.name for n in ast.walk(tree) if isinstance(n, ast.Import)
        for a in n.names
    ]
    assert imports == ["logging", "time", "pandas"]
    assert ast.get_docstring(tree).count("\n") == 2
    body = [type(n).__name__ for n in _method(tree, "_extract_").body]
    assert body == ["Expr", "Assign", "Pass", "Assign", "Expr"]
    assert _run_with_stub_pandas(code) == []


def test_step_names_are_emitted_as_literals():
    """Step names go into log/metrics calls as literals, not raw text."""
    builder, _ = _generate([{"source": "a.csv", "format": "csv"}])
    builder.steps[0].name = 'odd"); raise SystemExit("pwned"); ("'
    code = builder.generate_pipeline_code()
    assert not _has_raise(ast.parse(code))
    assert _run_with_stub_pandas(code) == [("csv", "a.csv")]


def test_profiled_file_with_hostile_name_round_trips():
    """A real file with a hostile name is profiled, generated and loaded."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, INJECTED_PATH)
        frame = pd.DataFrame({"customer_id": [1, 2], "name": ["a", "b"]})
        frame.to_csv(path, index=False)
        _, code = _generate([SourceProfiler().profile_auto(path)])
        namespace = {}
        exec(compile(code, "<generated>", "exec"), namespace)
        pipeline = namespace["GeneratedPipeline"]()
        pipeline.run()
        loaded = pipeline.dataframes["x____raise_systemexit__pwned______y"]
        assert list(loaded.columns) == ["customer_id", "name"]
        assert len(loaded) == 2


def test_regular_sources_generate_unchanged_method_names():
    """Ordinary sources keep their method names and read calls."""
    _, code = _generate([
        {"source": "/data/customers.csv", "format": "csv", "row_count": 1},
        {"source": "/data/orders.json", "format": "json", "row_count": 1},
    ])
    assert "    def _extract_customers(self):" in code
    assert (
        "self.dataframes['customers'] = pd.read_csv('/data/customers.csv')"
        in code
    )
    assert (
        "self.dataframes['orders'] = pd.read_json('/data/orders.json')"
        in code
    )
    assert "        self._consolidate_sources()" in code


if __name__ == "__main__":
    tests = [
        fn for name, fn in sorted(globals().items())
        if name.startswith("test_") and callable(fn)
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except (Exception, SystemExit) as exc:
            failed += 1
            print(f"FAIL {fn.__name__}: {type(exc).__name__}: {exc}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
