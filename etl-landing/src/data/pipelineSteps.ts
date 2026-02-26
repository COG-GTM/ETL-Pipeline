export interface PipelineStep {
  number: number;
  label: string;
  sublabel: string;
}

export const pipelineSteps: PipelineStep[] = [
  { number: 1, label: "Discover", sublabel: "Source Detection" },
  { number: 2, label: "Profile", sublabel: "Schema & Stats" },
  { number: 3, label: "Quality", sublabel: "6-D Scoring" },
  { number: 4, label: "Model", sublabel: "Star Schema" },
  { number: 5, label: "Generate", sublabel: "Pipeline Code" },
  { number: 6, label: "Orchestrate", sublabel: "Multi-Agent" },
  { number: 7, label: "Deploy", sublabel: "Production" },
];
