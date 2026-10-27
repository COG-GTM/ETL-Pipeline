import ast
import os
import sys
import tempfile
import types
import unittest

import pandas as pd

from src.pipeline_generator import PipelineBuilder
from src.profiler.source_profiler import SourceProfiler

INJECTED_PATH = 'x"); raise SystemExit("pwned"); ("y.csv'


def _generate(sources):
    builder = PipelineBuilder()
    builder.generate_pipeline_from_sources(sources, [], {})
    return builder, builder.generate_pipeline_code()


def _method(tree, prefix):
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
    matches = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name.startswith(prefix)]
    assert len(matches) == 1, [n.name for n in matches]
    return matches[0]


def _calls(node, attr):
    return [
        n for n in ast.walk(node)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == attr
    ]


def _run_with_stub_pandas(code):
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


class GeneratePipelineCodeTest(unittest.TestCase):
    def test_quote_in_source_path_stays_a_string_constant(self):
        for fmt, reader in (("csv", "read_csv"), ("json", "read_json")):
            with self.subTest(fmt=fmt):
                _, code = _generate([{"source": INJECTED_PATH, "format": fmt, "row_count": 1}])
                tree = ast.parse(code)
                method = _method(tree, "_extract_")
                calls = _calls(method, reader)
                self.assertEqual(len(calls), 1)
                self.assertEqual(len(calls[0].args), 1)
                self.assertIsInstance(calls[0].args[0], ast.Constant)
                self.assertEqual(calls[0].args[0].value, INJECTED_PATH)
                self.assertFalse(any(isinstance(n, ast.Raise) for n in ast.walk(tree)))

    def test_generated_code_reads_exact_path_without_running_payload(self):
        _, code = _generate([{"source": INJECTED_PATH, "format": "csv", "row_count": 1}])
        self.assertEqual(_run_with_stub_pandas(code), [("csv", INJECTED_PATH)])

    def test_newline_in_comment_values_cannot_start_code(self):
        source = "feed.xml\nraise SystemExit('pwned')\r__import__('os')"
        builder, _ = _generate([{"source": source, "format": "xml\nimport os", "row_count": 1}])
        builder.pipeline_config["generated_at"] = '"""\nraise SystemExit("pwned")\n"""'
        code = builder.generate_pipeline_code()
        tree = ast.parse(code)
        self.assertFalse(any(isinstance(n, ast.Raise) for n in ast.walk(tree)))
        imports = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        self.assertEqual(imports, ["logging", "time", "pandas"])
        self.assertEqual(ast.get_docstring(tree).count("\n"), 2)
        method = _method(tree, "_extract_")
        self.assertEqual(
            [type(n).__name__ for n in method.body], ["Expr", "Assign", "Pass", "Assign", "Expr"]
        )
        self.assertEqual(_run_with_stub_pandas(code), [])

    def test_step_names_are_emitted_as_literals(self):
        builder, _ = _generate([{"source": "a.csv", "format": "csv", "row_count": 1}])
        builder.steps[0].name = 'odd"); raise SystemExit("pwned"); ("'
        code = builder.generate_pipeline_code()
        tree = ast.parse(code)
        self.assertFalse(any(isinstance(n, ast.Raise) for n in ast.walk(tree)))
        self.assertEqual(_run_with_stub_pandas(code), [("csv", "a.csv")])

    def test_profiled_file_with_hostile_name_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, INJECTED_PATH)
            pd.DataFrame({"customer_id": [1, 2], "name": ["a", "b"]}).to_csv(path, index=False)
            profile = SourceProfiler().profile_auto(path)
            _, code = _generate([profile])
            namespace = {}
            exec(compile(code, "<generated>", "exec"), namespace)
            pipeline = namespace["GeneratedPipeline"]()
            pipeline.run()
            frame = pipeline.dataframes["x____raise_systemexit__pwned______y"]
            self.assertEqual(list(frame.columns), ["customer_id", "name"])
            self.assertEqual(len(frame), 2)

    def test_regular_sources_generate_unchanged_method_names(self):
        _, code = _generate([
            {"source": "/data/customers.csv", "format": "csv", "row_count": 1},
            {"source": "/data/orders.json", "format": "json", "row_count": 1},
        ])
        self.assertIn("    def _extract_customers(self):", code)
        self.assertIn("self.dataframes['customers'] = pd.read_csv('/data/customers.csv')", code)
        self.assertIn("self.dataframes['orders'] = pd.read_json('/data/orders.json')", code)
        self.assertIn("        self._consolidate_sources()", code)


if __name__ == "__main__":
    unittest.main()
