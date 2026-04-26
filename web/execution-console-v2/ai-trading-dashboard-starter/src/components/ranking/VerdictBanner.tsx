import type { RankingDetailDecision } from '@/lib/api/ranking';
import { cn } from '@/lib/utils/cn';

function verdictTone(verdict: string | null): { container: string; pill: string } {
  if (!verdict) {
    return {
      container: 'border-slate-700 bg-slate-900/60',
      pill: 'border-slate-700 bg-slate-800 text-slate-300',
    };
  }
  const upper = verdict.toUpperCase();
  if (upper.includes('BUY')) {
    return {
      container: 'border-emerald-500/30 bg-emerald-950/30',
      pill: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200',
    };
  }
  if (upper.includes('WATCH') || upper.includes('HOLD')) {
    return {
      container: 'border-amber-500/30 bg-amber-950/30',
      pill: 'border-amber-500/40 bg-amber-500/15 text-amber-200',
    };
  }
  if (upper.includes('REJECT') || upper.includes('BLOCK')) {
    return {
      container: 'border-rose-500/30 bg-rose-950/30',
      pill: 'border-rose-500/40 bg-rose-500/15 text-rose-200',
    };
  }
  return {
    container: 'border-blue-500/30 bg-blue-950/30',
    pill: 'border-blue-500/40 bg-blue-500/15 text-blue-200',
  };
}

export default function VerdictBanner({ decision }: { decision: RankingDetailDecision }) {
  const tone = verdictTone(decision.verdict);
  return (
    <div className={cn('flex items-center gap-4 rounded-xl border p-3', tone.container)}>
      <span
        className={cn(
          'rounded-md border px-3 py-1.5 text-xs font-bold uppercase tracking-widest',
          tone.pill,
        )}
      >
        {decision.verdict ?? 'PENDING'}
      </span>
      <div className="min-w-0 text-sm">
        <p className="font-semibold text-slate-200">
          Confidence: {decision.confidence ?? '—'}
        </p>
        <p className="text-slate-400">
          {decision.reason ?? 'No decision rationale available for this symbol.'}
        </p>
      </div>
    </div>
  );
}
