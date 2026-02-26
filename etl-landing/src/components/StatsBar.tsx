import { stats } from "@/data/stats";

const colorMap: Record<string, string> = {
  teal: "text-accent-teal",
  blue: "text-accent-blue",
  indigo: "text-accent-indigo",
  green: "text-accent-green",
};

export default function StatsBar() {
  return (
    <section>
      <div className="max-w-[1200px] mx-auto px-6">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-5 -mt-[30px] mb-15">
          {stats.map((stat) => (
            <div
              key={stat.label}
              className="bg-bg-card rounded-card p-6 text-center border border-[rgba(255,255,255,0.05)] transition-all duration-300 hover:translate-y-[-4px] hover:shadow-[0_12px_40px_rgba(0,0,0,0.3)] hover:border-[rgba(34,211,238,0.2)]"
            >
              <div
                className={`text-[42px] font-extrabold mb-1 ${colorMap[stat.colorClass]}`}
              >
                {stat.value}
              </div>
              <div className="text-[13px] text-text-muted uppercase tracking-[1.2px] font-semibold">
                {stat.label}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
