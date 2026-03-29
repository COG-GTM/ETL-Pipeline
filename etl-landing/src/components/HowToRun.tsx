export default function HowToRun() {
  return (
    <section className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Getting Started
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            Run the Demo
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Two commands. Zero configuration. The full ETL lifecycle in under 5
            seconds.
          </p>
        </div>
        <div className="bg-[#0D1117] rounded-[10px] p-5 overflow-x-auto my-6 border border-[rgba(255,255,255,0.05)]">
          <pre className="font-mono text-[13px] leading-[1.8] text-text-secondary">
            <span className="text-text-muted"># Install dependencies</span>
            {"\n"}
            <span className="text-accent-teal">pip</span> install -r
            requirements.txt{"\n"}
            {"\n"}
            <span className="text-text-muted">
              # Run the full interactive demo (press Enter to advance through
              phases)
            </span>
            {"\n"}
            <span className="text-accent-teal">python</span> -m demo.run_demo
            {"\n"}
            {"\n"}
            <span className="text-text-muted">
              # Or run in auto mode (non-interactive, no pauses)
            </span>
            {"\n"}
            <span className="text-accent-indigo">DEMO_AUTO_RUN</span>=
            <span className="text-accent-green">1</span>{" "}
            <span className="text-accent-teal">python</span> -m demo.run_demo
          </pre>
        </div>
        <div className="bg-linear-to-br from-[rgba(34,211,238,0.1)] to-[rgba(129,140,248,0.1)] border border-[rgba(34,211,238,0.2)] rounded-card p-8 mt-10">
          <h3 className="text-2xl font-bold mb-3">
            What Happens When You Run the Demo
          </h3>
          <p className="text-text-secondary text-[15px] leading-[1.7]">
            Devin automatically: (1) discovers and profiles 7 data sources
            across 4 formats, (2) runs 6-dimensional quality scoring on each,
            (3) designs a star schema with fact and dimension tables, (4)
            generates a 17-step pipeline with executable Python code, (5)
            coordinates 4 agent personas through the workflow, (6) demonstrates
            3 reusable playbooks, (7) learns the &quot;e-commerce&quot; domain
            from data patterns, and (8) consolidates 3 siloed systems into a
            unified dataset with full lineage. Total execution time: ~4 seconds.
          </p>
        </div>
      </div>
    </section>
  );
}
