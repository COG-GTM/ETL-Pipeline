export interface Playbook {
  title: string;
  description: string;
  steps: string[];
  accentColor: string;
}

export const playbooks: Playbook[] = [
  {
    title: "Data Onboarding",
    description:
      "End-to-end new source integration from discovery to production readiness.",
    steps: [
      "Discover source files",
      "Auto-profile all columns",
      "Detect and generate schema",
      "Run quality assessment",
      "Generate recommendations",
      "Produce onboarding report",
    ],
    accentColor: "accent-teal",
  },
  {
    title: "Quality Gate",
    description:
      "Configurable quality threshold enforcement that blocks bad data from entering pipelines.",
    steps: [
      "Load and profile data",
      "Run all quality checks",
      "Evaluate against thresholds",
      "Decision: APPROVED / CONDITIONAL / REJECTED",
      "Generate gate report",
      "Block on critical failures",
    ],
    accentColor: "accent-blue",
  },
  {
    title: "Cross-System Merge",
    description:
      "Multi-format data consolidation from siloed systems with lineage tracking.",
    steps: [
      "Register all data sources",
      "Profile each source independently",
      "Detect common join keys",
      "Execute consolidated merge",
      "Run post-merge quality checks",
      "Generate lineage report",
    ],
    accentColor: "accent-indigo",
  },
];
