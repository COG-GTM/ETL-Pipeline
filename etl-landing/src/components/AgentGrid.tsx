import { agents } from "@/data/agents";

const iconBgMap: Record<string, string> = {
  architect: "bg-linear-to-br from-[rgba(34,211,238,0.2)] to-[rgba(34,211,238,0.05)]",
  developer: "bg-linear-to-br from-[rgba(96,165,250,0.2)] to-[rgba(96,165,250,0.05)]",
  qa: "bg-linear-to-br from-[rgba(74,222,128,0.2)] to-[rgba(74,222,128,0.05)]",
  pm: "bg-linear-to-br from-[rgba(167,139,250,0.2)] to-[rgba(167,139,250,0.05)]",
};

export default function AgentGrid() {
  return (
    <section id="agents" className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Multi-Agent Orchestration
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            Four Specialized Personas
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Devin coordinates four agent personas in sequential phases, each
            with distinct responsibilities, decision-making authority, and
            complete audit trails.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-5">
          {agents.map((agent) => (
            <div
              key={agent.name}
              className="bg-bg-card rounded-card p-6 text-center border border-[rgba(255,255,255,0.05)] transition-all duration-300 hover:translate-y-[-4px] hover:shadow-[0_12px_40px_rgba(0,0,0,0.3)]"
            >
              <div
                className={`w-16 h-16 rounded-[16px] mx-auto mb-4 flex items-center justify-center text-[28px] ${iconBgMap[agent.iconClass]}`}
              >
                {agent.icon}
              </div>
              <h4 className="text-lg font-bold mb-1.5">{agent.name}</h4>
              <div className="text-xs text-accent-teal font-semibold uppercase tracking-[1px] mb-3">
                {agent.role}
              </div>
              <p className="text-[13px] text-text-secondary leading-[1.6]">
                {agent.description}
              </p>
            </div>
          ))}
        </div>

        <div className="bg-linear-to-br from-[rgba(34,211,238,0.1)] to-[rgba(129,140,248,0.1)] border border-[rgba(34,211,238,0.2)] rounded-card p-8 mt-10">
          <h3 className="text-2xl font-bold mb-3">
            Sequential Phase Handoffs
          </h3>
          <p className="text-text-secondary text-[15px] leading-[1.7]">
            Each agent completes their phase before handing off to the next. The
            Architect&apos;s schema design informs the Developer&apos;s pipeline
            code. The Developer&apos;s output feeds into QA&apos;s validation
            suite. The PM synthesizes all findings into a stakeholder-ready
            summary. Every action and decision is recorded in a full audit trail
            for compliance and traceability.
          </p>
        </div>
      </div>
    </section>
  );
}
