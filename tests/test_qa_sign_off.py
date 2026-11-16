import unittest

from src.orchestrator import MultiAgentOrchestrator


def _report(source, score, critical=0):
    return {
        "source": source,
        "overall_score": score,
        "grade": "A" if score >= 90 else "F",
        "total_checks": 10,
        "passed": 10,
        "failed": 0,
        "critical_failures": critical,
        "recommendations": [],
    }


def _profile(path):
    return {"source": path, "format": path.rsplit(".", 1)[-1], "row_count": 10, "column_count": 3}


def _run(reports, profiles):
    orchestrator = MultiAgentOrchestrator()
    result = orchestrator.run_orchestrated_workflow(profiles, reports, {}, {"steps": []})
    qa_actions = {a["action"]: a["details"] for a in result["agents"]["qa"]["actions_taken"]}
    return result, qa_actions


class QaSignOffTest(unittest.TestCase):
    def setUp(self):
        self.profiles = [
            _profile("sample_data/csv/customers.csv"),
            _profile("sample_data/csv/transactions.csv"),
            _profile("sample_data/csv/products.csv"),
        ]

    def test_one_failing_source_is_not_hidden_by_average(self):
        reports = [_report("customers", 0), _report("transactions", 100), _report("products", 100)]
        result, qa = _run(reports, self.profiles)

        self.assertFalse(result["qa_sign_off"]["approved"])
        self.assertFalse(qa["sign_off"]["approved"])
        self.assertEqual(qa["quality_summary"]["failing_sources"], ["customers"])
        self.assertEqual(result["status"], "failed")

    def test_critical_failure_blocks_sign_off(self):
        reports = [_report("customers", 95, critical=1), _report("transactions", 100), _report("products", 100)]
        result, _ = _run(reports, self.profiles)

        self.assertFalse(result["qa_sign_off"]["approved"])
        self.assertTrue(any("critical" in c for c in result["qa_sign_off"]["conditions"]))

    def test_profiled_source_without_report_blocks_sign_off(self):
        profiles = self.profiles + [_profile("sample_data/json/orders.json")]
        reports = [_report("customers", 100), _report("transactions", 100), _report("products", 100)]
        result, qa = _run(reports, profiles)

        self.assertFalse(result["qa_sign_off"]["approved"])
        self.assertEqual(qa["quality_summary"]["unassessed_sources"], ["sample_data/json/orders.json"])
        self.assertEqual(result["status"], "failed")

    def test_no_reports_is_not_approved(self):
        result, _ = _run([], self.profiles)

        self.assertFalse(result["qa_sign_off"]["approved"])

    def test_no_unverified_attestations_recorded(self):
        _, qa = _run([_report("customers", 100)], self.profiles[:1])

        self.assertNotIn("data_contracts_validated", qa["quality_summary"])
        self.assertNotIn("regression_tests_passed", qa["quality_summary"])

    def test_workflow_does_not_log_success_when_rejected(self):
        reports = [_report("customers", 0), _report("transactions", 100), _report("products", 100)]
        result, _ = _run(reports, self.profiles)

        events = [e["event"] for e in result["workflow_log"]]
        self.assertIn("workflow_failed", events)
        self.assertNotIn("workflow_completed", events)
        pm = {a["action"]: a["details"] for a in result["agents"]["pm"]["actions_taken"]}
        self.assertEqual(pm["progress_tracking"]["overall_status"], "blocked")

    def test_all_sources_pass_is_approved(self):
        reports = [_report("customers", 100), _report("transactions", 90), _report("products", 85)]
        result, _ = _run(reports, self.profiles)

        self.assertTrue(result["qa_sign_off"]["approved"])
        self.assertEqual(result["qa_sign_off"]["conditions"], [])
        self.assertEqual(result["status"], "completed")
        self.assertIn("workflow_completed", [e["event"] for e in result["workflow_log"]])


if __name__ == "__main__":
    unittest.main()
