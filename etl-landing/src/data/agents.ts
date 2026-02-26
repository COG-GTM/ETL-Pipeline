export interface Agent {
  icon: string;
  name: string;
  role: string;
  description: string;
  iconClass: string;
}

export const agents: Agent[] = [
  {
    icon: "\u{1F3D7}\u{FE0F}",
    name: "Architect",
    role: "Design Phase",
    description:
      "Analyzes source schemas, designs star schema target model, generates DDL, and makes structural decisions that downstream agents follow.",
    iconClass: "architect",
  },
  {
    icon: "\u{1F4BB}",
    name: "Developer",
    role: "Build Phase",
    description:
      "Builds extractors, implements transformations, creates loaders, and generates executable pipeline code based on the Architect's design.",
    iconClass: "developer",
  },
  {
    icon: "\u2705",
    name: "QA Engineer",
    role: "Validation Phase",
    description:
      "Runs quality assessments, validates data contracts, performs integration testing, and provides sign-off before production deployment.",
    iconClass: "qa",
  },
  {
    icon: "\u{1F4CA}",
    name: "Project Manager",
    role: "Coordination Phase",
    description:
      "Tracks progress across all phases, generates executive summaries, manages risk, and ensures delivery meets stakeholder requirements.",
    iconClass: "pm",
  },
];
