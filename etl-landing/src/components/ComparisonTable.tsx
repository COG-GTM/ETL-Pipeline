import { comparisonRows, type StatusType } from "@/data/comparison";

function StatusIcon({ status }: { status: StatusType }) {
  if (status === "check") {
    return <span className="text-accent-green font-bold text-base">&#10003;</span>;
  }
  if (status === "cross") {
    return <span className="text-accent-red font-bold text-base">&#10007;</span>;
  }
  return <span className="text-accent-blue font-semibold">~</span>;
}

export default function ComparisonTable() {
  return (
    <section id="comparison" className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Competitive Analysis
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            Devin vs Claude Code &amp; Other Tools
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Devin&apos;s zero-touch ETL goes beyond what current AI coding
            assistants offer for data engineering workflows.
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse rounded-card overflow-hidden">
            <thead>
              <tr>
                <th className="bg-bg-card text-text-header px-5 py-4 text-left text-[13px] font-bold uppercase tracking-[1px] border-b-2 border-[rgba(34,211,238,0.2)] w-[40%]">
                  Capability
                </th>
                <th className="bg-bg-card text-text-header px-5 py-4 text-left text-[13px] font-bold uppercase tracking-[1px] border-b-2 border-[rgba(34,211,238,0.2)] w-[30%]">
                  Devin
                </th>
                <th className="bg-bg-card text-text-header px-5 py-4 text-left text-[13px] font-bold uppercase tracking-[1px] border-b-2 border-[rgba(34,211,238,0.2)] w-[30%]">
                  Claude Code / Others
                </th>
              </tr>
            </thead>
            <tbody>
              {comparisonRows.map((row, i) => (
                <tr key={row.capability}>
                  <td
                    className={`px-5 py-3.5 text-sm font-semibold text-text-primary border-b border-[rgba(255,255,255,0.03)] ${
                      i % 2 === 0 ? "bg-bg-card" : "bg-bg-card-alt"
                    }`}
                  >
                    {row.capability}
                  </td>
                  <td
                    className={`px-5 py-3.5 text-sm border-b border-[rgba(255,255,255,0.03)] ${
                      i % 2 === 0 ? "bg-bg-card" : "bg-bg-card-alt"
                    }`}
                  >
                    <StatusIcon status={row.devinStatus} /> {row.devin}
                  </td>
                  <td
                    className={`px-5 py-3.5 text-sm border-b border-[rgba(255,255,255,0.03)] ${
                      i % 2 === 0 ? "bg-bg-card" : "bg-bg-card-alt"
                    }`}
                  >
                    <StatusIcon status={row.othersStatus} /> {row.others}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
