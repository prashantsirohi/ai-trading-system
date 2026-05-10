/**
 * Sector stage heatmap.
 *
 * Shows the same Weinstein S1-S4 distribution used on sector detail, but in a
 * compact ranked list so the sector overview is based on actual stage breadth
 * rather than the old capital-flow placeholder.
 */
import { Link } from 'react-router-dom';
import type { SectorScore, StockRow } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sectors: SectorScore[];
  rankedRows: StockRow[];
  selected: string | null;
  onSelect: (sector: string) => void;
}

function constituentCount(sector: string, rows: StockRow[]): number {
  return rows.filter((row) => row.sector === sector).length;
}

function quadrantTone(quadrant: string): string {
  const norm = quadrant.toLowerCase();
  if (norm === 'leading') return 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200';
  if (norm === 'improving') return 'border-blue-500/40 bg-blue-500/15 text-blue-200';
  if (norm === 'weakening') return 'border-amber-500/40 bg-amber-500/15 text-amber-200';
  if (norm === 'lagging') return 'border-rose-500/40 bg-rose-500/15 text-rose-200';
  return 'border-slate-700 bg-slate-800 text-slate-300';
}

const STAGES = [
  {
    key: 'S2',
    label: 'S2',
    name: 'Advancing',
    pct: (s: SectorScore) => s.stageS2Pct,
    count: (s: SectorScore) => s.stageS2Count,
    bar: 'bg-emerald-500',
    tile: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200',
  },
  {
    key: 'S1',
    label: 'S1',
    name: 'Base',
    pct: (s: SectorScore) => s.stageS1Pct,
    count: (s: SectorScore) => s.stageS1Count,
    bar: 'bg-blue-500',
    tile: 'border-blue-500/30 bg-blue-500/10 text-blue-200',
  },
  {
    key: 'S3',
    label: 'S3',
    name: 'Top',
    pct: (s: SectorScore) => s.stageS3Pct,
    count: (s: SectorScore) => s.stageS3Count,
    bar: 'bg-amber-500',
    tile: 'border-amber-500/30 bg-amber-500/10 text-amber-200',
  },
  {
    key: 'S4',
    label: 'S4',
    name: 'Decline',
    pct: (s: SectorScore) => s.stageS4Pct,
    count: (s: SectorScore) => s.stageS4Count,
    bar: 'bg-rose-500',
    tile: 'border-rose-500/30 bg-rose-500/10 text-rose-200',
  },
] as const;

function stageCount(s: SectorScore, stage: (typeof STAGES)[number]): number {
  const explicit = stage.count(s);
  if (explicit > 0 || s.stageTotal === 0) return explicit;
  return Math.round((stage.pct(s) / 100) * s.stageTotal);
}

export default function SectorLeadershipChart({ sectors, rankedRows, selected, onSelect }: Props) {
  if (sectors.length === 0) return null;

  return (
    <ul className="space-y-2">
      {sectors.map((s) => {
        const isSelected = selected === s.sector;
        const constituents = constituentCount(s.sector, rankedRows);
        const classified = s.stageTotal || constituents;
        return (
          <li key={s.sector}>
            <button
              type="button"
              onClick={() => onSelect(s.sector)}
              className={cn(
                'flex w-full flex-col gap-2 rounded-xl border p-3 text-left transition-colors',
                isSelected
                  ? 'border-blue-500/40 bg-blue-500/10'
                  : 'border-slate-800 bg-slate-950/60 hover:border-slate-600',
              )}
            >
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <Link
                    to={`/sectors/${encodeURIComponent(s.sector)}`}
                    onClick={(e) => e.stopPropagation()}
                    className="text-sm font-semibold text-slate-100 hover:text-blue-400 hover:underline transition-colors"
                  >
                    {s.sector}
                  </Link>
                  <span
                    className={cn(
                      'rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
                      quadrantTone(s.quadrant),
                    )}
                  >
                    {s.quadrant}
                  </span>
                </div>
                <div className="flex items-center gap-3 text-xs text-slate-400">
                  <span>
                    <span className="text-slate-500">Classified:</span>{' '}
                    <span className="font-semibold tabular-nums text-slate-200">{classified}</span>
                  </span>
                  <span>
                    <span className="text-slate-500">Rank:</span>{' '}
                    <span className="font-semibold tabular-nums text-slate-200">#{s.rank}</span>
                  </span>
                </div>
              </div>
              <div className="space-y-1">
                <div className="flex h-2 overflow-hidden rounded-full bg-slate-800">
                  {STAGES.map((stage) => (
                    <div
                      key={stage.key}
                      className={cn('h-full', stage.bar)}
                      style={{ width: `${Math.max(0, Math.min(100, stage.pct(s)))}%` }}
                      title={`${stage.label} ${stage.name}: ${stageCount(s, stage)} stocks`}
                    />
                  ))}
                </div>
                <div className="grid grid-cols-4 gap-1.5">
                  {STAGES.map((stage) => (
                    <div
                      key={stage.key}
                      className={cn('rounded-md border px-2 py-1', stage.tile)}
                      title={`${stage.label} ${stage.name}: ${stage.pct(s).toFixed(0)}%`}
                    >
                      <div className="flex items-baseline justify-between gap-1">
                        <span className="text-[10px] font-semibold uppercase">{stage.label}</span>
                        <span className="text-sm font-semibold tabular-nums">{stageCount(s, stage)}</span>
                      </div>
                      <div className="truncate text-[10px] text-slate-400">{stage.name}</div>
                    </div>
                  ))}
                </div>
              </div>
            </button>
          </li>
        );
      })}
    </ul>
  );
}
