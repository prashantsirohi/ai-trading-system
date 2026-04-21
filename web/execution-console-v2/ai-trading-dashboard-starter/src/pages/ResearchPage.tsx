import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';

export default function ResearchPage() {
  return (
    <PageFrame
      title="Research"
      description="Workspace for walk-forward validation, pattern studies, and promotion candidates."
    >
      <SectionCard title="Research Modules">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {[
            ['Walk-forward Ranker', 'Validate factor stability across rolling windows.'],
            ['Pattern Precision Study', 'Measure false positives for cup & handle and round bottom.'],
            ['Sector Rotation Notebook', 'Compare sector RS against NIFTY benchmarks.'],
          ].map(([title, text]) => (
            <div key={title} className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4">
              <div className="font-semibold">{title}</div>
              <div className="mt-1 text-sm text-slate-400">{text}</div>
            </div>
          ))}
        </div>
      </SectionCard>
    </PageFrame>
  );
}
