/**
 * Pattern catalog — named-pattern cards with live counts (proposal #04).
 *
 * Each card shows a wide SVG trace, state badge (confirmed / forming /
 * failed), symbols with active setups, and a 90-day historical hit rate.
 * Clicking a card narrows the setup grid below to that pattern type.
 *
 * Counts are derived client-side from the ranking/pattern feed. The
 * historical hit-rate stats are mock constants until a backtest endpoint
 * ships.
 */
import { cn } from '@/lib/utils/cn';
import type { StockRow } from '@/types/dashboard';
import {
  CupHandleIcon,
  TightFlagIcon,
  RoundBottomIcon,
  FlatBaseIcon,
  BreakoutIcon,
  AscendingTriangleIcon,
} from './PatternIcons';

export type CatalogState = 'confirmed' | 'forming' | 'failed' | 'idle';

export interface CatalogEntry {
  key: string;
  name: string;
  /** String patterns from StockRow.pattern that map to this catalog entry. */
  matches: string[];
  Icon: React.FC<{ size?: number }>;
  svgPath: React.ReactNode;
  hitRate90d: string;
  avgR: string;
}

// Inline SVG paths for the wide card preview (viewBox 200 × 70).
const CUP_PATH = (
  <path
    d="M0 20 Q 60 68, 100 45 T 160 22 L 175 28 L 195 18"
    stroke="var(--emerald-color, #34d399)"
    strokeWidth="1.6"
    fill="none"
  />
);
const FLAG_PATH = (
  <path
    d="M0 60 L 60 15 L 80 24 L 100 17 L 120 26 L 140 18 L 165 28 L 195 18"
    stroke="var(--amber-color, #fbbf24)"
    strokeWidth="1.6"
    fill="none"
  />
);
const ROUND_PATH = (
  <path
    d="M0 18 Q 60 68, 100 55 Q 140 68, 195 22"
    stroke="var(--emerald-color, #34d399)"
    strokeWidth="1.6"
    fill="none"
  />
);
const FLAT_PATH = (
  <path
    d="M0 50 L 40 54 L 80 50 L 120 52 L 160 48 L 195 50"
    stroke="var(--slate-color, #94a3b8)"
    strokeWidth="1.6"
    fill="none"
  />
);
const BREAKOUT_PATH = (
  <>
    <line x1="0" y1="34" x2="195" y2="34" stroke="#334155" strokeDasharray="3 6" strokeWidth="1" />
    <path
      d="M0 55 L 55 44 L 100 49 L 135 38 L 160 25 L 195 12"
      stroke="var(--emerald-color, #34d399)"
      strokeWidth="1.6"
      fill="none"
    />
  </>
);
const TRIANGLE_PATH = (
  <>
    <line x1="0" y1="16" x2="160" y2="16" stroke="#334155" strokeDasharray="3 4" strokeWidth="1" />
    <path
      d="M0 60 L 160 16 L 195 12"
      stroke="var(--rose-color, #fb7185)"
      strokeWidth="1.6"
      fill="none"
    />
  </>
);

export const CATALOG: CatalogEntry[] = [
  {
    key: 'cup',
    name: 'Cup & Handle',
    matches: ['cup'],
    Icon: CupHandleIcon,
    svgPath: CUP_PATH,
    hitRate90d: '62%',
    avgR: '+4.1R avg',
  },
  {
    key: 'flag',
    name: 'Tight Flag',
    matches: ['flag', 'tight flag'],
    Icon: TightFlagIcon,
    svgPath: FLAG_PATH,
    hitRate90d: '71%',
    avgR: '+2.8R avg',
  },
  {
    key: 'round',
    name: 'Round Bottom',
    matches: ['round'],
    Icon: RoundBottomIcon,
    svgPath: ROUND_PATH,
    hitRate90d: '54%',
    avgR: '+3.2R avg',
  },
  {
    key: 'flat',
    name: 'Flat Base',
    matches: ['flat base'],
    Icon: FlatBaseIcon,
    svgPath: FLAT_PATH,
    hitRate90d: '48%',
    avgR: '+1.9R avg',
  },
  {
    key: 'breakout',
    name: '52w Breakout',
    matches: ['breakout', '52w'],
    Icon: BreakoutIcon,
    svgPath: BREAKOUT_PATH,
    hitRate90d: '66%',
    avgR: '+5.4R avg',
  },
  {
    key: 'triangle',
    name: 'Ascending Triangle',
    matches: ['triangle', 'ascending'],
    Icon: AscendingTriangleIcon,
    svgPath: TRIANGLE_PATH,
    hitRate90d: '41%',
    avgR: '+1.1R avg',
  },
];

function patternToKey(pattern: string): string {
  const norm = pattern.toLowerCase();
  for (const entry of CATALOG) {
    if (entry.matches.some((m) => norm.includes(m))) return entry.key;
  }
  return '';
}

function deriveCatalogState(
  entry: CatalogEntry,
  rows: StockRow[],
): { state: CatalogState; symbols: string[]; count: number } {
  const matching = rows.filter(
    (r) => r.pattern && r.pattern !== 'N/A' && patternToKey(r.pattern) === entry.key,
  );
  const confirmed = matching.filter((r) => r.breakout);
  const forming = matching.filter((r) => !r.breakout);

  if (confirmed.length > 0)
    return { state: 'confirmed', symbols: confirmed.map((r) => r.symbol), count: confirmed.length };
  if (forming.length > 0)
    return { state: 'forming', symbols: forming.map((r) => r.symbol), count: forming.length };
  return { state: 'idle', symbols: [], count: 0 };
}

const STATE_BORDER: Record<CatalogState, string> = {
  confirmed: 'border-emerald-700/60',
  forming:   'border-amber-700/50',
  failed:    'border-rose-700/60',
  idle:      'border-slate-800',
};

const STATE_BADGE: Record<CatalogState, string> = {
  confirmed: 'border-emerald-700/60 bg-emerald-500/10 text-emerald-300',
  forming:   'border-amber-700/60 bg-amber-500/10 text-amber-300',
  failed:    'border-rose-700/60 bg-rose-500/15 text-rose-300',
  idle:      'border-slate-700 bg-slate-800/50 text-slate-400',
};

function stateLabel(state: CatalogState, count: number): string {
  if (count === 0) return 'idle';
  return `${count} ${state}`;
}

interface Props {
  rows: StockRow[];
  activeKey: string | null;
  onSelect: (key: string | null) => void;
}

export default function PatternCatalog({ rows, activeKey, onSelect }: Props) {
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3">
      {CATALOG.map((entry) => {
        const { state, symbols, count } = deriveCatalogState(entry, rows);
        const isActive = activeKey === entry.key;

        return (
          <button
            key={entry.key}
            type="button"
            onClick={() => onSelect(isActive ? null : entry.key)}
            className={cn(
              'flex flex-col overflow-hidden rounded-2xl border text-left transition-all',
              STATE_BORDER[state],
              isActive
                ? 'ring-2 ring-blue-500/40 ring-offset-1 ring-offset-slate-950'
                : 'hover:border-slate-600',
            )}
          >
            {/* SVG preview */}
            <svg
              viewBox="0 0 200 70"
              preserveAspectRatio="none"
              className="h-[70px] w-full bg-gradient-to-b from-slate-950/0 to-slate-950/60"
              aria-hidden="true"
            >
              {entry.svgPath}
            </svg>

            {/* Body */}
            <div className="bg-slate-900/60 p-3">
              <div className="flex flex-wrap items-center justify-between gap-1.5">
                <span className="text-sm font-semibold text-slate-100">{entry.name}</span>
                <span
                  className={cn(
                    'rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide',
                    STATE_BADGE[state],
                  )}
                >
                  {stateLabel(state, count)}
                </span>
              </div>

              {symbols.length > 0 ? (
                <p className="mt-1 truncate text-[11px] text-slate-400">
                  {symbols.join(' · ')}
                </p>
              ) : (
                <p className="mt-1 text-[11px] text-slate-600">No active setups</p>
              )}

              <div className="mt-2 flex items-center justify-between font-mono text-[10px] text-slate-500">
                <span>90d hit rate</span>
                <span className="text-slate-300">
                  {entry.hitRate90d} / {entry.avgR}
                </span>
              </div>
            </div>
          </button>
        );
      })}
    </div>
  );
}
