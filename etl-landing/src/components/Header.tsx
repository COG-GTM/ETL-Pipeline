export default function Header() {
  return (
    <header className="sticky top-0 z-50 backdrop-blur-[16px] bg-[rgba(15,23,42,0.95)] border-b border-[rgba(255,255,255,0.05)] py-4">
      <div className="max-w-[1200px] mx-auto px-6 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-linear-to-br from-accent-teal to-accent-indigo rounded-[10px] flex items-center justify-center font-extrabold text-lg text-white">
            D
          </div>
          <div className="text-xl font-bold tracking-[-0.5px]">
            <span className="bg-linear-to-r from-accent-teal to-accent-blue bg-clip-text text-transparent">
              Devin
            </span>{" "}
            Zero-Touch ETL
          </div>
        </div>
        <nav>
          <ul className="flex gap-6 list-none">
            <li>
              <a
                href="#pipeline"
                className="text-text-muted text-sm font-medium transition-colors duration-200 hover:text-text-primary hover:no-underline"
              >
                Pipeline
              </a>
            </li>
            <li>
              <a
                href="#agents"
                className="text-text-muted text-sm font-medium transition-colors duration-200 hover:text-text-primary hover:no-underline"
              >
                Agents
              </a>
            </li>
            <li>
              <a
                href="#playbooks"
                className="text-text-muted text-sm font-medium transition-colors duration-200 hover:text-text-primary hover:no-underline"
              >
                Playbooks
              </a>
            </li>
            <li>
              <a
                href="#comparison"
                className="text-text-muted text-sm font-medium transition-colors duration-200 hover:text-text-primary hover:no-underline"
              >
                Comparison
              </a>
            </li>
          </ul>
        </nav>
        <div className="bg-bg-card border border-[rgba(34,211,238,0.3)] rounded-[20px] px-4 py-1.5 text-[13px] text-accent-teal font-medium">
          Cognition AI
        </div>
      </div>
    </header>
  );
}
