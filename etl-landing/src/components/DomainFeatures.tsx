interface DomainFeature {
  icon: string;
  iconBg: string;
  title: string;
  description: string;
}

const features: DomainFeature[] = [
  {
    icon: "\u{1F50D}",
    iconBg: "bg-[rgba(34,211,238,0.15)]",
    title: "Auto Domain Detection",
    description:
      "Analyzes column names, data patterns, and entity relationships to automatically identify the business domain (e-commerce, healthcare, finance, logistics, manufacturing, retail).",
  },
  {
    icon: "\u{1F4D6}",
    iconBg: "bg-[rgba(96,165,250,0.15)]",
    title: "Domain Glossary",
    description:
      "Builds a comprehensive glossary of domain terms with definitions, data types, and relationships extracted directly from the data schemas.",
  },
  {
    icon: "\u2699\uFE0F",
    iconBg: "bg-[rgba(129,140,248,0.15)]",
    title: "Business Rule Inference",
    description:
      "Discovers implicit business rules from data patterns, including referential integrity constraints, value ranges, enumeration sets, and temporal dependencies.",
  },
  {
    icon: "\u{1F4BE}",
    iconBg: "bg-[rgba(74,222,128,0.15)]",
    title: "Persistent Knowledge",
    description:
      "All domain knowledge is persisted to disk and accumulated across sessions, enabling Devin to get faster and more accurate with each project in the same domain.",
  },
];

export default function DomainFeatures() {
  return (
    <section className="py-15">
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="text-center mb-12">
          <div className="inline-block text-xs text-accent-teal font-bold uppercase tracking-[2px] mb-3">
            Intelligence
          </div>
          <h2 className="text-4xl font-extrabold tracking-[-1px] mb-3">
            Domain Learning &amp; Greenfield Scenarios
          </h2>
          <p className="text-[17px] text-text-secondary max-w-[600px] mx-auto">
            Devin learns your business domain directly from the data, building
            persistent knowledge that accelerates future projects.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5 mt-8">
          {features.map((feature) => (
            <div
              key={feature.title}
              className="flex gap-4 items-start"
            >
              <div
                className={`w-10 h-10 rounded-[10px] flex items-center justify-center text-lg shrink-0 ${feature.iconBg}`}
              >
                {feature.icon}
              </div>
              <div>
                <h4 className="text-base font-bold mb-1">{feature.title}</h4>
                <p className="text-[13px] text-text-secondary">
                  {feature.description}
                </p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
