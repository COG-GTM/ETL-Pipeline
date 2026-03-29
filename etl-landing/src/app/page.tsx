import Header from "@/components/Header";
import Hero from "@/components/Hero";
import StatsBar from "@/components/StatsBar";
import PipelineFlow from "@/components/PipelineFlow";
import PhaseGrid from "@/components/PhaseGrid";
import QualityDimensions from "@/components/QualityDimensions";
import AgentGrid from "@/components/AgentGrid";
import PlaybookGrid from "@/components/PlaybookGrid";
import DomainFeatures from "@/components/DomainFeatures";
import HowToRun from "@/components/HowToRun";
import ComparisonTable from "@/components/ComparisonTable";
import Footer from "@/components/Footer";

export default function Home() {
  return (
    <>
      <Header />
      <Hero />
      <StatsBar />
      <PipelineFlow />
      <PhaseGrid />
      <QualityDimensions />
      <AgentGrid />
      <PlaybookGrid />
      <DomainFeatures />
      <HowToRun />
      <ComparisonTable />
      <Footer />
    </>
  );
}
