import { cn } from '@/lib/utils/cn';
import type { NewsEntry, NewsKind } from '@/lib/mock/symbolNews';

const KIND_PILL: Record<NewsKind, string> = {
  ALERT:  'border-amber-700/50 bg-amber-500/10 text-amber-300',
  NEWS:   'border-blue-700/50 bg-blue-500/10 text-blue-300',
  FILL:   'border-emerald-700/50 bg-emerald-500/10 text-emerald-300',
  SYSTEM: 'border-slate-700 bg-slate-800/60 text-slate-400',
};

interface Props {
  entries: NewsEntry[];
}

export default function NewsAndFillsPanel({ entries }: Props) {
  return (
    <div className="space-y-0 divide-y divide-slate-800/50">
      {entries.map((e, i) => (
        <div key={i} className="flex items-start gap-3 py-3">
          <span className="w-11 shrink-0 pt-0.5 font-mono text-[10px] text-slate-600">{e.ts}</span>
          <span
            className={cn(
              'shrink-0 rounded-full border px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide',
              KIND_PILL[e.kind],
            )}
          >
            {e.kind}
          </span>
          <span className="text-xs text-slate-300 leading-snug">{e.headline}</span>
        </div>
      ))}
      <p className="pt-3 text-[11px] text-slate-600 leading-relaxed">
        Live news and fill history will populate when the /api/stocks/:sym/events endpoint is wired.
      </p>
    </div>
  );
}
