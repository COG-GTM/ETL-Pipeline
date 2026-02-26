import { playbooks } from "@/data/playbooks";

const colorMap: Record<string, string> = {
  "accent-teal": "text-accent-teal",
  "accent-blue": "text-accent-blue",
  "accent-indigo": "text-accent-indigo",
};

const borderColorMap: Record<string, string> = {
  "accent-teal": "border-accent-teal",
  "accent-blue": "border-accent-blue",
  "accent-indigo": "border-accent-indigo",
};

export default function PlaybookGrid() {
  return (
    <section id="playbooks" className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Reusable Workflows
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            Playbook Templates
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Parameterized, composable workflows that encode data engineering
            best practices. Execute with a single command.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {playbooks.map((playbook) => (
            <div
              key={playbook.title}
              className="bg-bg-card rounded-card p-6 border border-[rgba(255,255,255,0.05)] transition-all duration-300 hover:border-[rgba(34,211,238,0.2)] hover:translate-y-[-2px]"
            >
              <h4
                className={`text-lg font-bold mb-2 ${colorMap[playbook.accentColor]}`}
              >
                {playbook.title}
              </h4>
              <p className="text-[13px] text-text-secondary leading-[1.7] mb-4">
                {playbook.description}
              </p>
              <ol className="list-none counter-reset-step">
                {playbook.steps.map((step, i) => (
                  <li
                    key={i}
                    className={`relative py-2 pl-9 text-[13px] text-text-secondary border-l-2 border-[rgba(255,255,255,0.05)] ml-3 before:content-[attr(data-step)] before:absolute before:left-[-13px] before:w-6 before:h-6 before:rounded-full before:bg-bg-primary before:border-2 ${borderColorMap[playbook.accentColor]} before:flex before:items-center before:justify-center before:text-[11px] before:font-bold ${colorMap[playbook.accentColor]}`}
                    data-step={i + 1}
                  >
                    {step}
                  </li>
                ))}
              </ol>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
