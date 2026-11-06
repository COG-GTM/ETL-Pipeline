import pytest

from src.orchestrator.multi_agent import AgentPersona, MultiAgentOrchestrator


class TestAgentPersona:
    """Tests for the AgentPersona class."""

    def test_init(self):
        agent = AgentPersona("Test Agent", "tester", ["test things"])
        assert agent.name == "Test Agent"
        assert agent.role == "tester"
        assert agent.responsibilities == ["test things"]
        assert agent.status == "idle"
        assert agent.actions_taken == []

    def test_record_action(self):
        agent = AgentPersona("Agent", "role", [])
        agent.record_action("analyze", {"source_count": 3})

        assert len(agent.actions_taken) == 1
        assert agent.actions_taken[0]["action"] == "analyze"
        assert agent.actions_taken[0]["details"]["source_count"] == 3
        assert "timestamp" in agent.actions_taken[0]

    def test_to_dict(self):
        agent = AgentPersona("Agent", "role", ["r1", "r2"])
        agent.status = "active"
        agent.record_action("test", {})

        d = agent.to_dict()
        assert d["name"] == "Agent"
        assert d["role"] == "role"
        assert d["responsibilities"] == ["r1", "r2"]
        assert d["status"] == "active"
        assert d["total_actions"] == 1

    def test_multiple_actions(self):
        agent = AgentPersona("Agent", "role", [])
        agent.record_action("step1", {"a": 1})
        agent.record_action("step2", {"b": 2})
        agent.record_action("step3", {"c": 3})

        assert len(agent.actions_taken) == 3
        assert agent.to_dict()["total_actions"] == 3


class TestMultiAgentOrchestrator:
    """Tests for the MultiAgentOrchestrator class."""

    def setup_method(self):
        self.orchestrator = MultiAgentOrchestrator()

    def test_init_default_agents(self):
        """Should initialize with 4 default agents."""
        assert "architect" in self.orchestrator.agents
        assert "developer" in self.orchestrator.agents
        assert "qa" in self.orchestrator.agents
        assert "pm" in self.orchestrator.agents

    def test_default_agents_properties(self):
        """Should have correct names and roles for default agents."""
        assert self.orchestrator.agents["architect"].role == "architect"
        assert self.orchestrator.agents["developer"].role == "developer"
        assert self.orchestrator.agents["qa"].role == "qa"
        assert self.orchestrator.agents["pm"].role == "pm"

    def test_default_agents_have_responsibilities(self):
        """Each default agent should have responsibilities."""
        for agent in self.orchestrator.agents.values():
            assert len(agent.responsibilities) > 0

    def test_run_orchestrated_workflow(self):
        """Should run complete workflow and return result."""
        result = self.orchestrator.run_orchestrated_workflow(
            source_profiles=[{"source": "test.csv", "row_count": 100, "column_count": 5, "format": "csv"}],
            quality_reports=[{"source": "test.csv", "overall_score": 85, "grade": "B", "total_checks": 10, "passed": 8, "failed": 2, "critical_failures": 0, "recommendations": []}],
            target_model={"schema_type": "star_schema", "fact_table": {"name": "fact_test"}, "dimension_tables": [], "relationships": []},
            pipeline_config={"steps": [{"type": "extract", "config": {"format": "csv"}}, {"type": "transform", "config": {"operations": ["clean"]}}, {"type": "load", "config": {}}], "total_steps": 3, "execution_config": {}},
        )

        assert "orchestration_completed_at" in result
        assert "agents" in result
        assert "workflow_log" in result
        assert result["total_events"] > 0

    def test_all_agents_completed_after_workflow(self):
        """All agents should have 'completed' status after workflow."""
        self.orchestrator.run_orchestrated_workflow(
            source_profiles=[{"source": "test.csv", "row_count": 100, "column_count": 5, "format": "csv"}],
            quality_reports=[{"source": "test.csv", "overall_score": 85, "grade": "B", "total_checks": 10, "passed": 8, "failed": 2, "critical_failures": 0, "recommendations": []}],
            target_model={"schema_type": "star_schema", "fact_table": {"name": "fact_test"}, "dimension_tables": [], "relationships": []},
            pipeline_config={"steps": [], "total_steps": 0, "execution_config": {}},
        )

        for agent in self.orchestrator.agents.values():
            assert agent.status == "completed"

    def test_architect_phase(self):
        """Should run architect phase and record actions."""
        profiles = [{"source": "test.csv", "row_count": 100, "column_count": 5, "format": "csv"}]
        target_model = {"schema_type": "star_schema", "fact_table": {"name": "fact_test"}, "dimension_tables": [{"name": "dim_a"}], "relationships": [{"type": "fk"}]}

        self.orchestrator._run_architect_phase(profiles, target_model)

        architect = self.orchestrator.agents["architect"]
        assert architect.status == "completed"
        assert len(architect.actions_taken) >= 3

    def test_developer_phase(self):
        """Should run developer phase and record actions."""
        pipeline_config = {
            "steps": [
                {"type": "extract", "config": {"format": "csv"}},
                {"type": "transform", "config": {"operations": ["clean"]}},
                {"type": "load", "config": {}},
            ],
        }

        self.orchestrator._run_developer_phase(pipeline_config)

        developer = self.orchestrator.agents["developer"]
        assert developer.status == "completed"
        assert len(developer.actions_taken) >= 3

    def test_qa_phase(self):
        """Should run QA phase and record quality assessments."""
        quality_reports = [
            {"source": "test.csv", "overall_score": 90, "grade": "B", "total_checks": 10, "passed": 9, "failed": 1, "critical_failures": 0, "recommendations": []},
        ]
        profiles = [{"source": "test.csv", "row_count": 100}]

        self.orchestrator._run_qa_phase(quality_reports, profiles)

        qa = self.orchestrator.agents["qa"]
        assert qa.status == "completed"
        assert len(qa.actions_taken) >= 2

    def test_qa_phase_sign_off_approved(self):
        """Should approve sign-off when avg score >= 60."""
        quality_reports = [
            {"source": "test.csv", "overall_score": 80, "grade": "B", "total_checks": 10, "passed": 8, "failed": 2, "critical_failures": 0, "recommendations": []},
        ]

        self.orchestrator._run_qa_phase(quality_reports, [])

        qa = self.orchestrator.agents["qa"]
        sign_off = [a for a in qa.actions_taken if a["action"] == "sign_off"][0]
        assert sign_off["details"]["approved"] is True

    def test_qa_phase_sign_off_rejected(self):
        """Should reject sign-off when avg score < 60."""
        quality_reports = [
            {"source": "test.csv", "overall_score": 40, "grade": "F", "total_checks": 10, "passed": 4, "failed": 6, "critical_failures": 2, "recommendations": []},
        ]

        self.orchestrator._run_qa_phase(quality_reports, [])

        qa = self.orchestrator.agents["qa"]
        sign_off = [a for a in qa.actions_taken if a["action"] == "sign_off"][0]
        assert sign_off["details"]["approved"] is False

    def test_pm_phase(self):
        """Should run PM phase and generate executive summary."""
        profiles = [{"source": "test.csv", "row_count": 100}]
        quality_reports = [{"source": "test.csv", "overall_score": 85}]
        target_model = {"schema_type": "star_schema"}
        pipeline_config = {"total_steps": 5}

        self.orchestrator._run_pm_phase(profiles, quality_reports, target_model, pipeline_config)

        pm = self.orchestrator.agents["pm"]
        assert pm.status == "completed"
        exec_summary = [a for a in pm.actions_taken if a["action"] == "executive_summary"]
        assert len(exec_summary) == 1

    def test_pm_risk_assessment_low(self):
        """Should assess low risk for high quality scores."""
        profiles = [{"source": "test.csv", "row_count": 100}]
        quality_reports = [{"source": "test.csv", "overall_score": 90}]

        self.orchestrator._run_pm_phase(profiles, quality_reports, {}, {"total_steps": 3})

        pm = self.orchestrator.agents["pm"]
        summary = [a for a in pm.actions_taken if a["action"] == "executive_summary"][0]
        assert summary["details"]["risk_assessment"] == "Low"

    def test_pm_risk_assessment_high(self):
        """Should assess high risk for low quality scores."""
        profiles = [{"source": "test.csv", "row_count": 100}]
        quality_reports = [{"source": "test.csv", "overall_score": 40}]

        self.orchestrator._run_pm_phase(profiles, quality_reports, {}, {"total_steps": 3})

        pm = self.orchestrator.agents["pm"]
        summary = [a for a in pm.actions_taken if a["action"] == "executive_summary"][0]
        assert summary["details"]["risk_assessment"] == "High"

    def test_log_event(self):
        """Should log events to workflow_log."""
        self.orchestrator._log_event("test_event", "TestAgent", "Test message")

        assert len(self.orchestrator.workflow_log) == 1
        assert self.orchestrator.workflow_log[0]["event"] == "test_event"
        assert self.orchestrator.workflow_log[0]["agent"] == "TestAgent"
        assert self.orchestrator.workflow_log[0]["message"] == "Test message"

    def test_generate_agent_report(self):
        """Should generate formatted report for a specific agent."""
        self.orchestrator.agents["architect"].record_action("test", {"key": "value"})
        report = self.orchestrator.generate_agent_report("architect")

        assert "AGENT REPORT" in report
        assert "Data Architect Agent" in report

    def test_generate_agent_report_unknown(self):
        """Should return error message for unknown agent."""
        report = self.orchestrator.generate_agent_report("nonexistent")
        assert "Unknown agent" in report

    def test_generate_orchestration_report(self):
        """Should generate full orchestration report."""
        self.orchestrator._log_event("test", "Agent", "message")
        report = self.orchestrator.generate_orchestration_report()

        assert "MULTI-AGENT ORCHESTRATION REPORT" in report
        assert "AGENT COORDINATION TIMELINE" in report
        assert "AGENT SUMMARY" in report
        assert "HANDOFF CHAIN" in report

    def test_workflow_log_tracks_phases(self):
        """Should track all phases in workflow log."""
        self.orchestrator.run_orchestrated_workflow(
            source_profiles=[{"source": "test.csv", "row_count": 100, "column_count": 5, "format": "csv"}],
            quality_reports=[{"source": "test.csv", "overall_score": 85, "grade": "B", "total_checks": 10, "passed": 8, "failed": 2, "critical_failures": 0, "recommendations": []}],
            target_model={"schema_type": "star_schema", "fact_table": {"name": "fact_test"}, "dimension_tables": [], "relationships": []},
            pipeline_config={"steps": [], "total_steps": 0, "execution_config": {}},
        )

        events = [e["event"] for e in self.orchestrator.workflow_log]
        assert "workflow_started" in events
        assert "workflow_completed" in events
        phase_starts = [e for e in events if e == "phase_started"]
        assert len(phase_starts) == 4  # architect, developer, qa, pm
