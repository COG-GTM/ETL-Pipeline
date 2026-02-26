export type StatusType = "check" | "cross" | "partial";

export interface ComparisonRow {
  capability: string;
  devin: string;
  devinStatus: StatusType;
  others: string;
  othersStatus: StatusType;
}

export const comparisonRows: ComparisonRow[] = [
  {
    capability: "Multi-agent orchestration",
    devin: "Native (4 personas with audit trails)",
    devinStatus: "check",
    others: "Not available",
    othersStatus: "cross",
  },
  {
    capability: "Persistent domain learning",
    devin: "Cross-session knowledge base",
    devinStatus: "check",
    others: "Stateless per session",
    othersStatus: "cross",
  },
  {
    capability: "Reusable playbooks",
    devin: "Built-in, parameterized, composable",
    devinStatus: "check",
    others: "Manual scripts only",
    othersStatus: "partial",
  },
  {
    capability: "Auto pipeline generation",
    devin: "End-to-end from data profiling",
    devinStatus: "check",
    others: "Partial, requires heavy guidance",
    othersStatus: "partial",
  },
  {
    capability: "Quality gate enforcement",
    devin: "Integrated in pipeline lifecycle",
    devinStatus: "check",
    others: "Requires separate tooling",
    othersStatus: "cross",
  },
  {
    capability: "Multi-format consolidation",
    devin: "Automatic (CSV/JSON/XML/API)",
    devinStatus: "check",
    others: "Manual per-format handling",
    othersStatus: "partial",
  },
  {
    capability: "Target model design",
    devin: "Auto DDL with star schema inference",
    devinStatus: "check",
    others: "Manual schema design required",
    othersStatus: "cross",
  },
  {
    capability: "Session persistence",
    devin: "Full state across sessions",
    devinStatus: "check",
    others: "No persistence",
    othersStatus: "cross",
  },
  {
    capability: "CI/CD integration",
    devin: "Native GitHub/GitLab integration",
    devinStatus: "check",
    others: "Limited integration",
    othersStatus: "partial",
  },
  {
    capability: "Greenfield onboarding",
    devin: "Zero-touch from raw data",
    devinStatus: "check",
    others: "Requires existing documentation",
    othersStatus: "cross",
  },
  {
    capability: "Data lineage tracking",
    devin: "Full lineage per transformation",
    devinStatus: "check",
    others: "No built-in lineage",
    othersStatus: "cross",
  },
];
