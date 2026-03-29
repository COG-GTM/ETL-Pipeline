export default function Hero() {
  return (
    <section className="py-20 pb-15 text-center">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="inline-block bg-[rgba(34,211,238,0.1)] border border-[rgba(34,211,238,0.2)] rounded-[20px] px-5 py-1.5 text-[13px] text-accent-teal font-semibold uppercase tracking-[1.5px] mb-6">
          Zero-Touch Data Engineering
        </div>
        <h1 className="text-[56px] max-md:text-[36px] font-extrabold tracking-[-2px] leading-[1.1] mb-5">
          End-to-End ETL
          <br />
          <span className="bg-linear-to-r from-accent-teal via-accent-blue to-accent-indigo bg-clip-text text-transparent">
            Fully Automated by Devin
          </span>
        </h1>
        <p className="text-xl text-text-secondary max-w-[700px] mx-auto mb-10 leading-[1.7]">
          From raw data discovery to production-ready pipelines, Devin automates
          the entire data engineering lifecycle with zero manual configuration.
          Multi-agent orchestration, intelligent quality gates, and persistent
          domain learning.
        </p>
        <div className="flex gap-4 justify-center">
          <a
            href="#pipeline"
            className="inline-flex items-center gap-2 px-7 py-3.5 rounded-[10px] text-[15px] font-semibold transition-all duration-300 hover:translate-y-[-2px] hover:shadow-[0_8px_32px_rgba(34,211,238,0.3)] hover:no-underline"
            style={{ background: "linear-gradient(135deg, #22D3EE, #60A5FA)", color: "#0F172A" }}
          >
            View the Pipeline
          </a>
          <a
            href="#comparison"
            className="inline-flex items-center gap-2 px-7 py-3.5 rounded-[10px] text-[15px] font-semibold bg-bg-card text-text-primary border border-[rgba(255,255,255,0.1)] transition-all duration-300 hover:border-accent-teal hover:no-underline"
          >
            Devin vs Claude Code
          </a>
        </div>
      </div>
    </section>
  );
}
