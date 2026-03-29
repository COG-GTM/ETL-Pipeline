import { phases } from "@/data/phases";

const phaseNumberStyles: Record<string, string> = {
  c1: "bg-[rgba(34,211,238,0.15)] text-accent-teal",
  c2: "bg-[rgba(96,165,250,0.15)] text-accent-blue",
  c3: "bg-[rgba(129,140,248,0.15)] text-accent-indigo",
  c4: "bg-[rgba(167,139,250,0.15)] text-accent-purple",
  c5: "bg-[rgba(74,222,128,0.15)] text-accent-green",
  c6: "bg-[rgba(248,113,113,0.15)] text-accent-red",
};

export default function PhaseGrid() {
  return (
    <section className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Demo Walkthrough
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            8 Phases of Zero-Touch ETL
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Each phase runs autonomously. Devin makes every decision, from
            format detection to DDL generation.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {phases.map((phase) => (
            <div
              key={phase.number}
              className="group bg-bg-card rounded-card p-6 border border-[rgba(255,255,255,0.05)] transition-all duration-300 relative overflow-hidden hover:border-[rgba(34,211,238,0.2)] hover:translate-y-[-2px] hover:shadow-[0_12px_40px_rgba(0,0,0,0.2)]"
            >
              <div className="absolute top-0 left-0 right-0 h-[3px] bg-linear-to-r from-accent-teal to-accent-indigo opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div
                className={`inline-flex items-center justify-center w-8 h-8 rounded-lg text-sm font-bold mb-4 ${phaseNumberStyles[phase.colorClass]}`}
              >
                {phase.number}
              </div>
              <h3 className="text-xl font-bold mb-2.5 tracking-[-0.3px]">
                {phase.title}
              </h3>
              <p className="text-text-secondary text-sm leading-[1.7] mb-4">
                {phase.description}
              </p>
              <div className="flex flex-wrap gap-1.5">
                {phase.tags.map((tag) => (
                  <span
                    key={tag}
                    className="bg-[rgba(255,255,255,0.05)] rounded-[6px] px-2.5 py-1 text-[11px] text-text-muted font-medium"
                  >
                    {tag}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
