import { pipelineSteps } from "@/data/pipelineSteps";

const stepColors = [
  { bg: "rgba(34,211,238,0.15)", border: "var(--color-accent-teal)", text: "var(--color-accent-teal)" },
  { bg: "rgba(96,165,250,0.15)", border: "var(--color-accent-blue)", text: "var(--color-accent-blue)" },
  { bg: "rgba(129,140,248,0.15)", border: "var(--color-accent-indigo)", text: "var(--color-accent-indigo)" },
  { bg: "rgba(167,139,250,0.15)", border: "var(--color-accent-purple)", text: "var(--color-accent-purple)" },
  { bg: "rgba(74,222,128,0.15)", border: "var(--color-accent-green)", text: "var(--color-accent-green)" },
  { bg: "rgba(34,211,238,0.15)", border: "var(--color-accent-teal)", text: "var(--color-accent-teal)" },
  { bg: "rgba(96,165,250,0.15)", border: "var(--color-accent-blue)", text: "var(--color-accent-blue)" },
];

export default function PipelineFlow() {
  return (
    <section id="pipeline" className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Automated Pipeline
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            The Zero-Touch ETL Lifecycle
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Devin orchestrates every stage of the data engineering lifecycle,
            from source discovery through production-ready pipeline deployment.
          </p>
        </div>
        <div className="flex items-center gap-0 py-10 overflow-x-auto max-md:flex-wrap">
          {pipelineSteps.map((step, i) => (
            <div key={step.number} className="contents">
              <div className="flex-1 min-w-[120px] text-center relative">
                <div
                  className="w-12 h-12 rounded-full mx-auto mb-3 flex items-center justify-center text-lg font-bold border-2"
                  style={{
                    background: stepColors[i].bg,
                    borderColor: stepColors[i].border,
                    color: stepColors[i].text,
                  }}
                >
                  {step.number}
                </div>
                <div className="text-xs font-semibold text-text-secondary">
                  {step.label}
                </div>
                <div className="text-[11px] text-text-muted mt-1">
                  {step.sublabel}
                </div>
              </div>
              {i < pipelineSteps.length - 1 && (
                <div className="w-10 flex items-center justify-center text-text-muted text-xl shrink-0 -mt-[30px]">
                  &rarr;
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
