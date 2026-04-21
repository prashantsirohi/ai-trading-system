import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';

export default function ExecutionPage() {
  return (
    <PageFrame
      title="Execution"
      description="Preview actions after trust, breakout linkage, and strategy filters."
    >
      <SectionCard title="Execution Queue">
        <div className="space-y-3">
          {[
            ['RELIANCE', 'BUY', 'Tier A breakout + strong sector'],
            ['INFY', 'WATCH', 'Pattern active, breakout confirmation pending'],
            ['BEL', 'BUY', 'Defence leadership + high volume'],
          ].map(([symbol, action, note]) => (
            <div key={symbol} className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4">
              <div className="flex items-center justify-between">
                <span className="font-semibold">{symbol}</span>
                <span className="text-sm text-emerald-400">{action}</span>
              </div>
              <div className="mt-1 text-sm text-slate-400">{note}</div>
            </div>
          ))}
        </div>
      </SectionCard>
    </PageFrame>
  );
}
