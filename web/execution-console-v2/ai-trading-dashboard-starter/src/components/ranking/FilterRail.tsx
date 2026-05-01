/**
 * Ranking · Filter rail (proposal #01).
 *
 * Left-rail multi-facet filter for the Ranking view: saved views, score &
 * RS range sliders, tier toggles, sector multi-select, and setup flags.
 * Filtering itself is applied by the parent — the rail is purely
 * controlled.
 */
import { useMemo, useState } from 'react';

import { cn } from '@/lib/utils/cn';
import {
  DEFAULT_FILTER_STATE,
  RS_BOUNDS,
  SCORE_BOUNDS,
  SEED_SAVED_VIEWS,
  isDefaultState,
  statesEqual,
  type RankingFilterState,
  type SavedView,
  type Tier,
} from '@/lib/storage/rankingViews';
import type { StockRow } from '@/types/dashboard';

const TIERS: Tier[] = ['A', 'B', 'C'];
const TIER_TONE: Record<Tier, string> = {
  A: 'border-emerald-700 bg-emerald-900/40 text-emerald-300',
  B: 'border-blue-800 bg-blue-900/40 text-blue-300',
  C: 'border-amber-800 bg-amber-900/30 text-amber-300',
};

interface Counts {
  tiers: Record<Tier, number>;
  sectors: Map<string, number>;
  breakouts: number;
  patterns: number;
}

function computeCounts(rows: StockRow[]): Counts {
  const tiers: Record<Tier, number> = { A: 0, B: 0, C: 0 };
  const sectors = new Map<string, number>();
  let breakouts = 0;
  let patterns = 0;
  for (const row of rows) {
    if (row.tier in tiers) tiers[row.tier as Tier] += 1;
    const sector = row.sector ?? 'Unknown';
    sectors.set(sector, (sectors.get(sector) ?? 0) + 1);
    if (row.breakout) breakouts += 1;
    if (row.pattern && row.pattern !== 'N/A') patterns += 1;
  }
  return { tiers, sectors, breakouts, patterns };
}

interface Props {
  state: RankingFilterState;
  onChange: (next: RankingFilterState) => void;
  rows: StockRow[];
  customViews: SavedView[];
  onSaveView: (name: string) => void;
  onDeleteView: (id: string) => void;
  onSelectView: (view: SavedView) => void;
}

export default function FilterRail({
  state,
  onChange,
  rows,
  customViews,
  onSaveView,
  onDeleteView,
  onSelectView,
}: Props) {
  const counts = useMemo(() => computeCounts(rows), [rows]);
  const sortedSectors = useMemo(
    () =>
      [...counts.sectors.entries()].sort(
        (a, b) => b[1] - a[1] || a[0].localeCompare(b[0]),
      ),
    [counts.sectors],
  );

  const allViews = useMemo<SavedView[]>(
    () => [...SEED_SAVED_VIEWS, ...customViews],
    [customViews],
  );

  const activeViewId = useMemo(
    () => allViews.find((v) => statesEqual(v.state, state))?.id ?? null,
    [allViews, state],
  );

  const [savePromptOpen, setSavePromptOpen] = useState(false);
  const [saveName, setSaveName] = useState('');

  const handleSave = () => {
    const name = saveName.trim();
    if (!name) return;
    onSaveView(name);
    setSaveName('');
    setSavePromptOpen(false);
  };

  const toggleTier = (tier: Tier) => {
    const next = state.tiers.includes(tier)
      ? state.tiers.filter((t) => t !== tier)
      : [...state.tiers, tier];
    onChange({ ...state, tiers: next });
  };

  const toggleSector = (sector: string) => {
    const next = state.sectors.includes(sector)
      ? state.sectors.filter((s) => s !== sector)
      : [...state.sectors, sector];
    onChange({ ...state, sectors: next });
  };

  return (
    <aside className="rounded-xl border border-slate-800 bg-slate-950/50 p-3.5">
      {/* Saved Views */}
      <RailSection title="Saved Views" first>
        <ul className="flex flex-col gap-1">
          {allViews.map((view) => {
            const active = view.id === activeViewId;
            return (
              <li key={view.id} className="group relative">
                <button
                  type="button"
                  onClick={() => onSelectView(view)}
                  className={cn(
                    'flex w-full items-center justify-between gap-2 rounded-md px-2 py-1.5 text-left text-xs transition-colors',
                    active
                      ? 'border border-blue-500/40 bg-blue-500/10 text-white'
                      : 'border border-transparent text-slate-300 hover:bg-slate-800/40',
                  )}
                >
                  <span className="flex items-center gap-2 truncate">
                    {active ? (
                      <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-blue-400" />
                    ) : null}
                    <span className="truncate">{view.name}</span>
                  </span>
                  {!view.builtin ? (
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        onDeleteView(view.id);
                      }}
                      aria-label={`Delete saved view ${view.name}`}
                      className="text-[10px] text-slate-500 opacity-0 transition-opacity hover:text-rose-300 group-hover:opacity-100"
                    >
                      ×
                    </button>
                  ) : null}
                </button>
              </li>
            );
          })}
        </ul>
        {savePromptOpen ? (
          <div className="mt-2 flex flex-col gap-1.5">
            <input
              autoFocus
              value={saveName}
              onChange={(e) => setSaveName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleSave();
                if (e.key === 'Escape') {
                  setSaveName('');
                  setSavePromptOpen(false);
                }
              }}
              placeholder="View name…"
              className="w-full rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1 text-xs text-slate-200 placeholder:text-slate-500 focus:border-blue-500/60 focus:outline-none"
            />
            <div className="flex gap-1.5">
              <button
                type="button"
                onClick={handleSave}
                className="flex-1 rounded-md border border-blue-500/40 bg-blue-500/15 px-2 py-1 text-[11px] font-semibold text-blue-200 hover:border-blue-500/60"
              >
                Save
              </button>
              <button
                type="button"
                onClick={() => {
                  setSaveName('');
                  setSavePromptOpen(false);
                }}
                className="rounded-md border border-slate-700 px-2 py-1 text-[11px] text-slate-400 hover:border-slate-500"
              >
                Cancel
              </button>
            </div>
          </div>
        ) : (
          <button
            type="button"
            onClick={() => setSavePromptOpen(true)}
            disabled={isDefaultState(state)}
            className={cn(
              'mt-2 w-full rounded-md border border-dashed px-2 py-1.5 text-left text-xs transition-colors',
              isDefaultState(state)
                ? 'cursor-not-allowed border-slate-800 text-slate-600'
                : 'border-slate-700 text-slate-400 hover:border-blue-500/40 hover:text-blue-200',
            )}
          >
            + Save current view
          </button>
        )}
      </RailSection>

      {/* Score */}
      <RailSection title="Score">
        <RangeSlider
          min={SCORE_BOUNDS[0]}
          max={SCORE_BOUNDS[1]}
          step={1}
          value={state.scoreRange}
          onChange={(next) => onChange({ ...state, scoreRange: next })}
          format={(v) => v.toFixed(0)}
        />
      </RailSection>

      {/* Relative Strength */}
      <RailSection title="Relative Strength">
        <RangeSlider
          min={RS_BOUNDS[0]}
          max={RS_BOUNDS[1]}
          step={1}
          value={state.rsRange}
          onChange={(next) => onChange({ ...state, rsRange: next })}
          format={(v) => v.toFixed(0)}
        />
      </RailSection>

      {/* Tier */}
      <RailSection title="Tier">
        <ul className="flex flex-col gap-1">
          {TIERS.map((tier) => {
            const checked = state.tiers.includes(tier);
            return (
              <li key={tier}>
                <button
                  type="button"
                  onClick={() => toggleTier(tier)}
                  className="flex w-full items-center justify-between rounded-md px-1 py-1 text-xs text-slate-300 hover:bg-slate-800/40"
                >
                  <span className="flex items-center gap-2">
                    <CheckBox checked={checked} />
                    <span
                      className={cn(
                        'inline-flex h-5 w-5 items-center justify-center rounded text-[10px] font-bold',
                        TIER_TONE[tier],
                      )}
                    >
                      {tier}
                    </span>
                  </span>
                  <span className="font-mono text-[10px] text-slate-500">
                    {counts.tiers[tier]}
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      </RailSection>

      {/* Sector */}
      {sortedSectors.length > 0 ? (
        <RailSection title="Sector">
          <ul className="flex max-h-60 flex-col gap-0.5 overflow-y-auto pr-1">
            {sortedSectors.map(([sector, count]) => {
              const checked = state.sectors.includes(sector);
              return (
                <li key={sector}>
                  <button
                    type="button"
                    onClick={() => toggleSector(sector)}
                    className="flex w-full items-center justify-between gap-2 rounded-md px-1 py-1 text-left text-xs text-slate-300 hover:bg-slate-800/40"
                  >
                    <span className="flex min-w-0 items-center gap-2">
                      <CheckBox checked={checked} />
                      <span className="truncate">{sector}</span>
                    </span>
                    <span className="font-mono text-[10px] text-slate-500">{count}</span>
                  </button>
                </li>
              );
            })}
          </ul>
        </RailSection>
      ) : null}

      {/* Setup */}
      <RailSection title="Setup">
        <ul className="flex flex-col gap-1">
          <li>
            <button
              type="button"
              onClick={() =>
                onChange({ ...state, breakoutOnly: !state.breakoutOnly })
              }
              className="flex w-full items-center justify-between rounded-md px-1 py-1 text-xs text-slate-300 hover:bg-slate-800/40"
            >
              <span className="flex items-center gap-2">
                <CheckBox checked={state.breakoutOnly} />
                <span>Breakout confirmed</span>
              </span>
              <span className="font-mono text-[10px] text-slate-500">
                {counts.breakouts}
              </span>
            </button>
          </li>
          <li>
            <button
              type="button"
              onClick={() =>
                onChange({ ...state, hasPatternOnly: !state.hasPatternOnly })
              }
              className="flex w-full items-center justify-between rounded-md px-1 py-1 text-xs text-slate-300 hover:bg-slate-800/40"
            >
              <span className="flex items-center gap-2">
                <CheckBox checked={state.hasPatternOnly} />
                <span>Has pattern</span>
              </span>
              <span className="font-mono text-[10px] text-slate-500">
                {counts.patterns}
              </span>
            </button>
          </li>
        </ul>
      </RailSection>

      <button
        type="button"
        onClick={() => onChange({ ...DEFAULT_FILTER_STATE })}
        disabled={isDefaultState(state)}
        className={cn(
          'mt-4 w-full rounded-md border px-2 py-1.5 text-[11px] font-semibold uppercase tracking-wider transition-colors',
          isDefaultState(state)
            ? 'cursor-not-allowed border-slate-800 text-slate-600'
            : 'border-rose-500/40 text-rose-300 hover:border-rose-500/60 hover:bg-rose-500/10',
        )}
      >
        Reset filters
      </button>
    </aside>
  );
}

function RailSection({
  title,
  first,
  children,
}: {
  title: string;
  first?: boolean;
  children: React.ReactNode;
}) {
  return (
    <section className={cn(first ? 'mt-0' : 'mt-3.5')}>
      <h4 className="mb-2 text-[11px] font-semibold uppercase tracking-[0.1em] text-slate-500">
        {title}
      </h4>
      {children}
    </section>
  );
}

function CheckBox({ checked }: { checked: boolean }) {
  return (
    <span
      aria-hidden
      className={cn(
        'inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-[4px] border text-[9px] font-bold leading-none',
        checked
          ? 'border-emerald-500 bg-emerald-500 text-slate-950'
          : 'border-slate-700 bg-transparent',
      )}
    >
      {checked ? '✓' : ''}
    </span>
  );
}

interface RangeSliderProps {
  min: number;
  max: number;
  step: number;
  value: [number, number];
  onChange: (next: [number, number]) => void;
  format: (n: number) => string;
}

function RangeSlider({ min, max, step, value, onChange, format }: RangeSliderProps) {
  const [low, high] = value;
  const span = max - min;
  const leftPct = ((low - min) / span) * 100;
  const rightPct = 100 - ((high - min) / span) * 100;

  const setLow = (next: number) => {
    const clamped = Math.min(next, high);
    onChange([clamped, high]);
  };
  const setHigh = (next: number) => {
    const clamped = Math.max(next, low);
    onChange([low, clamped]);
  };

  return (
    <div className="px-1 pt-1">
      <div className="relative h-4">
        <div className="absolute inset-x-0 top-1.5 h-1 rounded-full bg-slate-800" />
        <div
          className="absolute top-1.5 h-1 rounded-full bg-blue-500"
          style={{ left: `${leftPct}%`, right: `${rightPct}%` }}
        />
        <input
          aria-label="Minimum"
          type="range"
          min={min}
          max={max}
          step={step}
          value={low}
          onChange={(e) => setLow(Number(e.target.value))}
          className="range-thumb absolute inset-x-0 top-0 h-4 w-full appearance-none bg-transparent"
        />
        <input
          aria-label="Maximum"
          type="range"
          min={min}
          max={max}
          step={step}
          value={high}
          onChange={(e) => setHigh(Number(e.target.value))}
          className="range-thumb absolute inset-x-0 top-0 h-4 w-full appearance-none bg-transparent"
        />
      </div>
      <div className="mt-1 flex justify-between font-mono text-[10px] text-slate-500">
        <span>{format(low)}</span>
        <span>{format(high)}</span>
      </div>
    </div>
  );
}
