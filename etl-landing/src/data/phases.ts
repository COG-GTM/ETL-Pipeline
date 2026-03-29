export interface Phase {
  number: number;
  title: string;
  description: string;
  tags: string[];
  colorClass: string;
}

export const phases: Phase[] = [
  {
    number: 1,
    title: "Source Data Understanding & Profiling",
    description:
      "Devin auto-discovers and connects to data sources regardless of format (CSV, JSON, XML, API). It performs statistical profiling, semantic type detection (emails, phones, dates, IDs), and infers column roles (identifier, categorical, text, numeric). All profiling happens without any manual schema mapping.",
    tags: ["CSV", "JSON", "XML", "API", "Auto-Schema", "DDL Generation"],
    colorClass: "c1",
  },
  {
    number: 2,
    title: "Data Quality Measurement",
    description:
      "Comprehensive quality scoring across six dimensions: completeness, uniqueness, consistency, validity, timeliness, and accuracy. Each source receives a 0-100 quality score with a letter grade (A-F). The engine intelligently distinguishes primary keys from foreign keys, avoiding false penalties.",
    tags: [
      "Completeness",
      "Uniqueness",
      "Consistency",
      "Validity",
      "Timeliness",
      "Accuracy",
    ],
    colorClass: "c2",
  },
  {
    number: 3,
    title: "Target Data Model Design",
    description:
      "Automatically translates source data profiles into an optimized target model. Devin infers star schemas (fact vs dimension tables), detects foreign key relationships, designs aggregation tables for analytics, and adds SCD Type 2 slowly-changing dimension support with full audit columns.",
    tags: ["Star Schema", "FK Detection", "SCD Type 2", "Aggregations", "DDL"],
    colorClass: "c3",
  },
  {
    number: 4,
    title: "Automated Pipeline Development",
    description:
      "Pipeline configuration and executable Python code are auto-generated from the combination of source profiles, quality reports, and target model. Extraction strategy adapts to data volume (full load vs chunked). Cleaning steps are derived directly from quality findings.",
    tags: [
      "Auto-Generated",
      "Python",
      "Retry Logic",
      "Parallelism",
      "Validation",
    ],
    colorClass: "c4",
  },
  {
    number: 5,
    title: "Multi-Agent Orchestration",
    description:
      "Four specialized agent personas coordinate sequentially, each with distinct responsibilities and a full audit trail. The Architect designs, the Developer builds, QA validates, and the PM tracks progress. Every action is logged for complete traceability.",
    tags: ["Architect", "Developer", "QA", "PM", "Audit Trail"],
    colorClass: "c5",
  },
  {
    number: 6,
    title: "Reusable Playbooks",
    description:
      "Three executable playbook templates that encode best practices: Data Onboarding for new source integration, Quality Gate for threshold enforcement, and Cross-System Merge for multi-format consolidation. Playbooks are parameterized and composable.",
    tags: [
      "Onboarding",
      "Quality Gate",
      "Cross-System Merge",
      "Composable",
    ],
    colorClass: "c6",
  },
  {
    number: 7,
    title: "Domain Learning & Greenfield",
    description:
      'Devin learns the business domain directly from the data: auto-detects the domain (e-commerce, healthcare, finance), discovers entities and relationships, infers business rules from data patterns, builds a domain glossary, and persists all knowledge for future sessions.',
    tags: [
      "Auto-Detection",
      "Glossary",
      "Business Rules",
      "Persistent Knowledge",
    ],
    colorClass: "c1",
  },
  {
    number: 8,
    title: "Data Consolidation",
    description:
      "Merges data from multiple siloed systems in different formats (CSV flat files, JSON APIs, XML enterprise feeds, API responses). Full data lineage tracking through every load, join, and transformation ensures complete auditability.",
    tags: [
      "Multi-Format",
      "Lineage Tracking",
      "Join Detection",
      "Auditability",
    ],
    colorClass: "c2",
  },
];
