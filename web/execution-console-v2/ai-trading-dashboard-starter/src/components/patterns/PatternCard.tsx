/**
 * Pattern card — Canvas-style summary of a pattern candidate.
 *
 * Surfaces:
 *
 *   * Pattern-type SVG glyph (`PatternIcons.patternIconFor`).
 *   * Urgency heat (🔥 IMMINENT / ⚠️ NEAR / ⏳ EARLY) derived from row
 *     state. The backend doesn't expose distance-to-breakout yet, so we
 *     compose urgency from the breakout flag + score band.
 *   * Quality tier label, RS, sector strength, and a derived
 *     distance-to-breakout estimate.
 *   * Failure risk (low / med / high) with a rationale.
 */
import type { StockRow } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';
import { patternIconFor } from './PatternIcons';

type Urgency = 'imminent' | 'near' | 'early';

interface UrgencyDef {
  key: Urgency;
  label: string;
  emoji: string;
  tone: string;
}

const URGENCY: Record<Urgency, UrgencyDef> = {
  imminent: { key: 'imminent', label: 'Imminent', emoji: '🔥', tone: 'border-rose-500/40 bg-rose-500/15 text-rose-200' },
  near: { key: 'near', label: 'Near', emoji: '⚠️', tone: 'border-amber-500/40 bg-amber-500/15 text-amber-200' },
  early: { key: 'early', label: 'Early', emoji: '⏳', tone: 'border-slate-600 bg-slate-800/60 text-slate-300' },
};

function urgencyFor(row: StockRow): UrgencyDef {
  if (row.breakout) return URGENCY.imminent;
  if (row.score >= 7.5 || row.rs >= 80) return URGENCY.near;
  return URGENCY.early;
}

function distanceToBreakout(row: StockRow): string {
  if (row.breakout) return 'At breakout';
  // Heuristic: lower RS = further from breakout. Returns a +%-style hint.
  const gap = Math.max(0.5, Math.min(12, (90 - row.rs) * 0.15));
  return `~${gap.toFixed(1)}% to pivot`;
}

interface FailureAssessment {
  label: 'Low' | 'Medium' | 'High';
  tone: string;
  reason: string;
}

function failureAssessment(row: StockRow): FailureAssessment {
  if (row.tier === 'A' && row.sectorStrength >= 75) {
    return {
      label: 'Low',
      tone: 'text-emerald-300',
      reason: 'Tier-A leadership with a strong sector tailwind.',
    };
  }
  if (row.tier === 'C' || row.sectorStrength < 55) {
    return {
      label: 'High',
      tone: 'text-rose-300',
      reason: 'Weak tier or thin sector breadth — fade risk elevated.',
    };
  }
  return {
    label: 'Medium',
    tone: 'text-amber-300',
    reason: 'Setup viable but awaiting volume + breadth confirmation.',
  };
}

interface Props {
  row: StockRow;
  onSelect?: (row: StockRow) => void;
}

export default function PatternCard({ row, onSelect }: Props) {
  const Icon = patternIconFor(row.pattern);
  const urgency = urgencyFor(row);
  const failure = failureAssessment(row);
  const distance = distanceToBreakout(row);

  return (
    <button
      type="button"
      onClick={() => onSelect?.(row)}
      className={cn(
        'flex flex-col gap-3 rounded-2xl border border-slate-800 bg-slate-950/60 p-4 text-left transition-colors',
        'hover:border-slate-600 hover:bg-slate-900/60',
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-3">
          <div className="flex h-12 w-16 items-center justify-center rounded-lg border border-slate-700 bg-slate-900/80 text-slate-300">
            <Icon size={32} />
          </div>
          <div>
            <div className="text-sm font-semibold text-slate-100">{row.symbol}</div>
            <div className="text-xs text-slate-400">
              {row.pattern && row.pattern !== 'N/A' ? row.pattern : 'Pattern pending'} · {row.sector}
            </div>
          </div>
        </div>
        <span
          className={cn(
            'shrink-0 rounded-full border px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider',
            urgency.tone,
          )}
        >
          {urgency.emoji} {urgency.label}
        </span>
      </div>

      <dl className="grid grid-cols-3 gap-2 text-xs">
        <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-2">
          <dt className="text-[10px] uppercase tracking-wider text-slate-500">Tier</dt>
          <dd className="mt-0.5 font-semibold text-slate-100">Tier {row.tier}</dd>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-2">
          <dt className="text-[10px] uppercase tracking-wider text-slate-500">RS</dt>
          <dd className="mt-0.5 font-semibold tabular-nums text-slate-100">{row.rs}</dd>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-2">
          <dt className="text-[10px] uppercase tracking-wider text-slate-500">Sector RS</dt>
          <dd className="mt-0.5 font-semibold tabular-nums text-slate-100">{row.sectorStrength}</dd>
        </div>
      </dl>

      <div className="flex items-center justify-between text-xs">
        <span className="text-slate-400">
          <span className="text-slate-500">Distance: </span>
          <span className="font-semibold text-slate-200">{distance}</span>
        </span>
        <span className="text-slate-400">
          <span className="text-slate-500">Failure risk: </span>
          <span className={cn('font-semibold', failure.tone)}>{failure.label}</span>
        </span>
      </div>
      <p className="text-[11px] leading-snug text-slate-500">{failure.reason}</p>
    </button>
  );
}
