export interface Stat {
  value: string;
  label: string;
  colorClass: string;
}

export const stats: Stat[] = [
  { value: "8", label: "Automated Phases", colorClass: "teal" },
  { value: "4", label: "Agent Personas", colorClass: "blue" },
  { value: "7", label: "Data Sources Profiled", colorClass: "indigo" },
  { value: "0", label: "Manual Config Steps", colorClass: "green" },
];
