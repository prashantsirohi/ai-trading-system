/**
 * Output Summary cards row — the four navigation cards on the Control
 * Tower (Top Ranked / Breakouts / Patterns / Sector Leaders). Clicking
 * a card jumps to the relevant tab.
 */
import { useNavigate } from 'react-router-dom';

import { ArrowUpRightIcon } from './icons';
import type { WorkspaceSnapshot } from '@/lib/api/workspace';
import { cn } from '@/lib/utils/cn';

interface Props {
  snapshot: WorkspaceSnapshot | undefined;
}

interface CardSpec {
  label: string;
  count: number;
  to: string;
  highlight: string | null;
  tone: 'blue' | 'emerald' | 'purple' | 'amber';
}

const TONE_RING: Record<CardSpec['tone'], string> = {
  blue: 'hover:border-blue-500/50 hover:shadow-[0_0_20px_rgba(59,130,246,0.15)]',
  emerald: 'hover:border-emerald-500/50 hover:shadow-[0_0_20px_rgba(16,185,129,0.15)]',
  purple: 'hover:border-purple-500/50 hover:shadow-[0_0_20px_rgba(168,85,247,0.15)]',
  amber: 'hover:border-amber-500/50 hover:shadow-[0_0_20px_rgba(245,158,11,0.15)]',
};

const TONE_TEXT: Record<CardSpec['tone'], string> = {
  blue: 'text-blue-300',
  emerald: 'text-emerald-300',
  purple: 'text-purple-300',
  amber: 'text-amber-300',
};

export default function OutputSummaryCards({ snapshot }: Props) {
  const navigate = useNavigate();
  const counts = snapshot?.counts ?? { ranked: 0, breakouts: 0, patterns: 0, sectors: 0 };

  const cards: CardSpec[] = [
    {
      label: 'Top Ranked',
      count: counts.ranked,
      to: '/ranking',
      highlight: snapshot?.topActions[0]?.symbol ?? null,
      tone: 'blue',
    },
    {
      label: 'Breakouts',
      count: counts.breakouts,
      to: '/patterns',
      highlight: snapshot?.summary.topSector ?? null,
      tone: 'emerald',
    },
    {
      label: 'Pattern Setups',
      count: counts.patterns,
      to: '/patterns',
      highlight: null,
      tone: 'purple',
    },
    {
      label: 'Sector Leaders',
      count: counts.sectors,
      to: '/sectors',
      highlight: snapshot?.sectorLeaders[0]?.sector ?? null,
      tone: 'amber',
    },
  ];

  return (
    <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
      {cards.map((card) => (
        <button
          key={card.label}
          type="button"
          onClick={() => navigate(card.to)}
          className={cn(
            'group flex flex-col rounded-2xl border border-slate-800 bg-slate-950/60 p-4 text-left transition-all',
            TONE_RING[card.tone],
          )}
        >
          <div className="flex items-start justify-between">
            <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500">
              {card.label}
            </span>
            <ArrowUpRightIcon
              size={14}
              className={cn(
                'text-slate-500 transition-colors',
                'group-hover:' + TONE_TEXT[card.tone],
              )}
            />
          </div>
          <div className="mt-4 flex items-end justify-between">
            <span className="text-4xl font-light text-white tabular-nums">{card.count}</span>
          </div>
          <span className="mt-3 truncate text-xs text-slate-400">
            {card.highlight ? (
              <>
                Lead: <span className={TONE_TEXT[card.tone]}>{card.highlight}</span>
              </>
            ) : (
              <span className="text-slate-600">No lead yet</span>
            )}
          </span>
        </button>
      ))}
    </div>
  );
}
