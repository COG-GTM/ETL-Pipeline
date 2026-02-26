export default function Footer() {
  return (
    <>
      <section className="py-15">
        <div className="max-w-[1200px] mx-auto px-6 text-center">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Ready to See It Live?
          </div>
          <h2 className="text-4xl font-extrabold mb-4">
            Experience Zero-Touch ETL
          </h2>
          <p className="text-text-secondary text-[17px] max-w-[600px] mx-auto mb-8">
            Run the demo yourself. See Devin profile your data, design your
            schema, build your pipeline, and orchestrate your agents, all
            without writing a single line of configuration.
          </p>
          <div className="flex gap-4 justify-center">
            <a
              href="https://github.com/COG-GTM/ETL-Pipeline/pull/35"
              className="inline-flex items-center gap-2 px-7 py-3.5 rounded-[10px] text-[15px] font-semibold bg-linear-to-br from-accent-teal to-accent-blue text-[#0F172A] transition-all duration-300 hover:translate-y-[-2px] hover:shadow-[0_8px_32px_rgba(34,211,238,0.3)] hover:no-underline"
              target="_blank"
              rel="noopener noreferrer"
            >
              View the PR
            </a>
            <a
              href="https://app.devin.ai"
              className="inline-flex items-center gap-2 px-7 py-3.5 rounded-[10px] text-[15px] font-semibold bg-bg-card text-text-primary border border-[rgba(255,255,255,0.1)] transition-all duration-300 hover:border-accent-teal hover:no-underline"
              target="_blank"
              rel="noopener noreferrer"
            >
              Try Devin
            </a>
          </div>
        </div>
      </section>

      <footer className="py-12 border-t border-[rgba(255,255,255,0.05)] mt-15 text-center">
        <div className="max-w-[1200px] mx-auto px-6">
          <p className="text-text-muted text-[13px]">
            Devin Zero-Touch ETL Demo &mdash; Built by Cognition AI &mdash;
            End-to-End Data Engineering Automation
          </p>
        </div>
      </footer>
    </>
  );
}
