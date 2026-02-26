interface QualityItem {
  title: string;
  titleColor: string;
  description: string;
  barWidth: string;
  barGradient: string;
}

const qualityItems: QualityItem[] = [
  {
    title: "Completeness",
    titleColor: "text-accent-teal",
    description:
      "Null analysis per-column and overall. Flags missing data hotspots and recommends imputation strategies.",
    barWidth: "92%",
    barGradient: "from-accent-teal to-accent-blue",
  },
  {
    title: "Uniqueness",
    titleColor: "text-accent-blue",
    description:
      "Duplicate detection with intelligent PK vs FK distinction. Only true primary keys are flagged for uniqueness violations.",
    barWidth: "88%",
    barGradient: "from-accent-blue to-accent-indigo",
  },
  {
    title: "Consistency",
    titleColor: "text-accent-indigo",
    description:
      "Value pattern analysis detects near-duplicates, casing inconsistencies, and leading/trailing whitespace across string columns.",
    barWidth: "75%",
    barGradient: "from-accent-indigo to-accent-purple",
  },
  {
    title: "Validity",
    titleColor: "text-accent-purple",
    description:
      "Format validation for emails, phone numbers, and numeric ranges. Detects negative values in financial columns.",
    barWidth: "70%",
    barGradient: "from-accent-purple to-accent-red",
  },
  {
    title: "Timeliness",
    titleColor: "text-accent-green",
    description:
      "Detects future dates and stale data. Ensures temporal integrity across all date and timestamp columns.",
    barWidth: "95%",
    barGradient: "from-accent-green to-accent-teal",
  },
  {
    title: "Accuracy",
    titleColor: "text-accent-red",
    description:
      "Statistical outlier detection using IQR method. Flags data points that deviate significantly from expected distributions.",
    barWidth: "82%",
    barGradient: "from-accent-red to-accent-purple",
  },
];

export default function QualityDimensions() {
  return (
    <section className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Data Quality
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            Six-Dimensional Quality Scoring
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Every data source is evaluated across six quality dimensions,
            producing a composite score that gates pipeline execution.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-8">
          {qualityItems.map((item) => (
            <div key={item.title} className="bg-bg-card rounded-card p-5">
              <h5 className={`text-[15px] font-bold mb-1.5 ${item.titleColor}`}>
                {item.title}
              </h5>
              <p className="text-xs text-text-muted leading-[1.5]">
                {item.description}
              </p>
              <div className="h-1.5 bg-[rgba(255,255,255,0.05)] rounded-[3px] mt-3 overflow-hidden">
                <div
                  className={`h-full rounded-[3px] bg-linear-to-r ${item.barGradient}`}
                  style={{ width: item.barWidth }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
