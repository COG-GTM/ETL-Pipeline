import os
from datetime import datetime
from typing import Any

QA_MIN_SOURCE_SCORE = 60
QA_CLEAN_SOURCE_SCORE = 80


def _unassessed_profiles(profiles: list[dict[str, Any]], quality_reports: list[dict[str, Any]]) -> list[str]:
    """Return profiled sources not covered by a report; each report covers at most one profile.

    A report can cover a profile whose basename, with or without extension, equals the report's
    source (e.g. report "customers" covers "data/customers.csv"). Reports are assigned with a
    maximum bipartite matching (exact basename tried first) so the result does not depend on report order.
    """
    names = [os.path.basename(str(p.get("source") or "")).lower() for p in profiles]
    keys = [os.path.basename(str(qr.get("source", ""))).lower() for qr in quality_reports]
    candidates = [
        [i for i, n in enumerate(names) if n and n == key]
        + [i for i, n in enumerate(names) if n and n != key and os.path.splitext(n)[0] == key]
        for key in keys
    ]
    owner: dict[int, int] = {}

    def assign(report: int, seen: set[int]) -> bool:
        """Try to give ``report`` a profile, moving earlier owners along augmenting paths.

        ``seen`` holds profile indices already visited in this search. Returns True if assigned.
        """
        for i in candidates[report]:
            if i not in seen:
                seen.add(i)
                if i not in owner or assign(owner[i], seen):
                    owner[i] = report
                    return True
        return False

    for report in range(len(keys)):
        assign(report, set())
    return [p.get("source") or f"profile #{i}" for i, p in enumerate(profiles) if i not in owner]


class AgentPersona:
    def __init__(self, name: str, role: str, responsibilities: list[str]):
        self.name = name
        self.role = role
        self.responsibilities = responsibilities
        self.actions_taken: list[dict[str, Any]] = []
        self.status = "idle"

    def record_action(self, action: str, details: dict[str, Any]) -> None:
        self.actions_taken.append({
            "action": action,
            "details": details,
            "timestamp": datetime.now().isoformat(),
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "role": self.role,
            "responsibilities": self.responsibilities,
            "status": self.status,
            "actions_taken": self.actions_taken,
            "total_actions": len(self.actions_taken),
        }


class MultiAgentOrchestrator:
    def __init__(self):
        self.agents: dict[str, AgentPersona] = {}
        self.workflow_log: list[dict[str, Any]] = []
        self._init_default_agents()

    def _init_default_agents(self) -> None:
        self.agents["architect"] = AgentPersona(
            "Data Architect Agent",
            "architect",
            [
                "Analyze source data structures and schemas",
                "Design target data models (star schema, snowflake, data vault)",
                "Define entity relationships and cardinality",
                "Recommend indexing and partitioning strategies",
                "Evaluate data modeling trade-offs (normalization vs denormalization)",
                "Produce DDL scripts and ER diagrams",
            ],
        )

        self.agents["developer"] = AgentPersona(
            "Pipeline Developer Agent",
            "developer",
            [
                "Build extraction logic for each source system",
                "Implement data transformations and business rules",
                "Generate pipeline orchestration code",
                "Handle error recovery and retry logic",
                "Optimize query performance and memory usage",
                "Implement incremental load patterns (CDC, watermark)",
            ],
        )

        self.agents["qa"] = AgentPersona(
            "Quality Assurance Agent",
            "qa",
            [
                "Run data profiling and quality assessments",
                "Validate data completeness, accuracy, and consistency",
                "Execute regression tests on pipeline outputs",
                "Monitor data drift and schema changes",
                "Generate quality scorecards and trend reports",
                "Define and enforce data contracts",
            ],
        )

        self.agents["pm"] = AgentPersona(
            "Project Manager Agent",
            "pm",
            [
                "Track pipeline development progress and milestones",
                "Coordinate handoffs between agent personas",
                "Generate status reports and executive summaries",
                "Manage risk assessment and mitigation",
                "Prioritize backlog of data integration tasks",
                "Estimate effort and timeline for new pipelines",
            ],
        )

    def run_orchestrated_workflow(
        self,
        source_profiles: list[dict[str, Any]],
        quality_reports: list[dict[str, Any]],
        target_model: dict[str, Any],
        pipeline_config: dict[str, Any],
    ) -> dict[str, Any]:
        self._log_event("workflow_started", "PM", "Orchestrated ETL workflow initiated")

        self._run_architect_phase(source_profiles, target_model)
        self._run_developer_phase(pipeline_config)
        sign_off = self._run_qa_phase(quality_reports, source_profiles)
        self._run_pm_phase(source_profiles, quality_reports, target_model, pipeline_config, sign_off)

        if sign_off["approved"]:
            status = "completed"
            self._log_event("workflow_completed", "PM", "All phases completed; QA sign-off approved")
        else:
            status = "failed"
            self._log_event(
                "workflow_failed", "PM",
                "QA sign-off rejected: " + "; ".join(sign_off["conditions"]),
            )

        return {
            "status": status,
            "qa_sign_off": sign_off,
            "orchestration_completed_at": datetime.now().isoformat(),
            "agents": {name: agent.to_dict() for name, agent in self.agents.items()},
            "workflow_log": self.workflow_log,
            "total_events": len(self.workflow_log),
        }

    def _run_architect_phase(self, profiles: list[dict[str, Any]], target_model: dict[str, Any]) -> None:
        architect = self.agents["architect"]
        architect.status = "active"
        self._log_event("phase_started", "Architect", "Source analysis and model design phase")

        architect.record_action("analyze_sources", {
            "sources_analyzed": len(profiles),
            "total_columns": sum(p.get("column_count", 0) for p in profiles),
            "total_rows": sum(p.get("row_count", 0) for p in profiles),
            "formats_detected": list(set(p.get("format", "unknown") for p in profiles)),
        })

        if target_model:
            fact = target_model.get("fact_table", {})
            dims = target_model.get("dimension_tables", [])
            rels = target_model.get("relationships", [])
            architect.record_action("design_target_model", {
                "schema_type": target_model.get("schema_type", "star_schema"),
                "fact_table": fact.get("name", "N/A") if fact else "N/A",
                "dimension_count": len(dims),
                "relationships_defined": len(rels),
            })

            architect.record_action("generate_ddl", {
                "tables_created": 1 + len(dims),
                "indexes_recommended": sum(
                    len(p.get("indexes_recommended", []))
                    for p in profiles
                ),
            })

        architect.record_action("review_complete", {
            "assessment": "Source data structures analyzed, target model designed with SCD Type 2 dimensions",
            "risk_level": "low",
        })

        architect.status = "completed"
        self._log_event("phase_completed", "Architect", "Model design finalized")

    def _run_developer_phase(self, pipeline_config: dict[str, Any]) -> None:
        developer = self.agents["developer"]
        developer.status = "active"
        self._log_event("phase_started", "Developer", "Pipeline development phase")

        steps = pipeline_config.get("steps", [])
        extract_steps = [s for s in steps if s.get("type") == "extract"]
        transform_steps = [s for s in steps if s.get("type") == "transform"]
        load_steps = [s for s in steps if s.get("type") == "load"]

        developer.record_action("build_extractors", {
            "extractors_built": len(extract_steps),
            "formats_handled": list(set(
                s.get("config", {}).get("format", "unknown") for s in extract_steps
            )),
            "strategy": "parallel_extraction",
        })

        developer.record_action("build_transformations", {
            "transform_steps": len(transform_steps),
            "operations": [
                op
                for s in transform_steps
                for op in s.get("config", {}).get("operations", ["model_transform"])
            ],
        })

        developer.record_action("build_loaders", {
            "loaders_built": len(load_steps),
            "write_mode": "upsert",
            "audit_columns_added": True,
        })

        developer.record_action("generate_pipeline_code", {
            "total_steps": len(steps),
            "execution_config": pipeline_config.get("execution_config", {}),
            "code_generated": True,
        })

        developer.status = "completed"
        self._log_event("phase_completed", "Developer", "Pipeline code generated and reviewed")

    def _run_qa_phase(
        self, quality_reports: list[dict[str, Any]], profiles: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Record per-source QA results and return a sign-off that requires every profiled source to pass."""
        qa = self.agents["qa"]
        qa.status = "active"
        self._log_event("phase_started", "QA", "Quality assurance and validation phase")

        conditions: list[str] = []
        failing_sources: list[str] = []
        for qr in quality_reports:
            source = qr.get("source", "unknown")
            score = qr.get("overall_score", 0)
            critical = qr.get("critical_failures", 0)
            qa.record_action("quality_assessment", {
                "source": source,
                "score": score,
                "grade": qr.get("grade", "F"),
                "checks_run": qr.get("total_checks", 0),
                "checks_passed": qr.get("passed", 0),
                "checks_failed": qr.get("failed", 0),
                "critical_issues": critical,
            })
            if score < QA_MIN_SOURCE_SCORE:
                failing_sources.append(source)
                conditions.append(f"{source}: score {score} below minimum {QA_MIN_SOURCE_SCORE}")
            if critical > 0:
                if source not in failing_sources:
                    failing_sources.append(source)
                conditions.append(f"{source}: {critical} critical quality failure(s)")

        unassessed_sources = _unassessed_profiles(profiles, quality_reports)
        for source in unassessed_sources:
            conditions.append(f"{source}: profiled but no quality report")
        if not quality_reports:
            conditions.append("No quality reports provided")

        all_scores = [qr.get("overall_score", 0) for qr in quality_reports]
        avg_score = sum(all_scores) / len(all_scores) if all_scores else 0
        min_score = min(all_scores) if all_scores else 0

        approved = not conditions
        if approved and min_score < QA_CLEAN_SOURCE_SCORE:
            conditions.append("Address quality issues before production deployment")

        qa.record_action("quality_summary", {
            "sources_profiled": len(profiles),
            "sources_assessed": len(quality_reports),
            "average_quality_score": round(avg_score, 1),
            "minimum_quality_score": min_score,
            "failing_sources": failing_sources,
            "unassessed_sources": unassessed_sources,
            "recommendation_count": sum(len(qr.get("recommendations", [])) for qr in quality_reports),
        })

        sign_off = {"approved": approved, "conditions": conditions}
        qa.record_action("sign_off", sign_off)

        qa.status = "completed"
        self._log_event(
            "phase_completed", "QA",
            f"Quality assessment complete (avg score: {avg_score:.1f}, "
            f"sign-off: {'approved' if approved else 'rejected'})",
        )
        return sign_off

    def _run_pm_phase(
        self,
        profiles: list[dict[str, Any]],
        quality_reports: list[dict[str, Any]],
        target_model: dict[str, Any],
        pipeline_config: dict[str, Any],
        sign_off: dict[str, Any],
    ) -> None:
        pm = self.agents["pm"]
        pm.status = "active"
        self._log_event("phase_started", "PM", "Project management review phase")

        pm.record_action("progress_tracking", {
            "phases_completed": ["architect", "developer", "qa"],
            "overall_status": "on_track" if sign_off["approved"] else "blocked",
            "milestone": (
                "Pipeline design and validation complete"
                if sign_off["approved"]
                else "Blocked on QA sign-off"
            ),
        })

        total_rows = sum(p.get("row_count", 0) for p in profiles)
        total_sources = len(profiles)
        avg_quality = sum(qr.get("overall_score", 0) for qr in quality_reports) / len(quality_reports) if quality_reports else 0

        pm.record_action("executive_summary", {
            "total_data_sources": total_sources,
            "total_records_processed": total_rows,
            "average_data_quality": round(avg_quality, 1),
            "pipeline_steps": pipeline_config.get("total_steps", 0),
            "target_model_type": target_model.get("schema_type", "N/A") if target_model else "N/A",
            "estimated_runtime_minutes": max(1, total_rows // 10000),
            "risk_assessment": "Low" if avg_quality >= 80 else "Medium" if avg_quality >= 60 else "High",
        })

        pm.record_action("recommendations", {
            "items": [
                "Schedule pipeline for off-peak hours to minimize system impact",
                "Set up monitoring dashboards for pipeline health metrics",
                "Implement alerting for quality score degradation",
                "Plan for incremental load pattern after initial full load",
                "Document data lineage for compliance and audit requirements",
            ],
        })

        pm.status = "completed"
        self._log_event("phase_completed", "PM", "Project review and sign-off complete")

    def _log_event(self, event_type: str, agent: str, message: str) -> None:
        self.workflow_log.append({
            "event": event_type,
            "agent": agent,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        })

    def generate_agent_report(self, agent_name: str) -> str:
        if agent_name not in self.agents:
            return f"Unknown agent: {agent_name}"

        agent = self.agents[agent_name]
        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  AGENT REPORT: {agent.name}")
        lines.append(f"{'='*70}")
        lines.append(f"  Role: {agent.role.upper()}")
        lines.append(f"  Status: {agent.status}")
        lines.append(f"  Total Actions: {len(agent.actions_taken)}")
        lines.append("")

        lines.append("  RESPONSIBILITIES:")
        for r in agent.responsibilities:
            lines.append(f"    - {r}")
        lines.append("")

        lines.append("  ACTIONS TAKEN:")
        lines.append(f"  {'─'*50}")
        for action in agent.actions_taken:
            lines.append(f"  [{action['timestamp'][:19]}] {action['action']}")
            for k, v in action["details"].items():
                lines.append(f"    {k}: {v}")
            lines.append("")

        return "\n".join(lines)

    def generate_orchestration_report(self) -> str:
        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  MULTI-AGENT ORCHESTRATION REPORT")
        lines.append(f"{'='*70}")
        lines.append("")

        lines.append("  AGENT COORDINATION TIMELINE:")
        lines.append(f"  {'─'*50}")
        for event in self.workflow_log:
            lines.append(f"  [{event['timestamp'][:19]}] [{event['agent']:12s}] {event['message']}")
        lines.append("")

        lines.append("  AGENT SUMMARY:")
        lines.append(f"  {'─'*50}")
        for name, agent in self.agents.items():
            status_icon = "++" if agent.status == "completed" else "--" if agent.status == "active" else "  "
            lines.append(f"  {status_icon} {agent.name:30s} Status: {agent.status:10s} Actions: {len(agent.actions_taken)}")

        lines.append("")
        lines.append("  HANDOFF CHAIN:")
        roles = ["architect", "developer", "qa", "pm"]
        for i, role in enumerate(roles):
            agent = self.agents.get(role)
            if agent:
                arrow = " --> " if i < len(roles) - 1 else ""
                lines.append(f"  {agent.name}{arrow}")

        return "\n".join(lines)
