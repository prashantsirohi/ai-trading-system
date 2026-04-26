/**
 * Active-filter chip strip rendered above the ranking table (proposal #01).
 *
 * Summarises the active filter axes as removable pills. Each chip clears
 * just its own axis; "Clear all" resets to the default state.
 */
import {
  DEFAULT_FILTER_STATE,
  RS_BOUNDS,
  SCORE_BOUNDS,
  type RankingFilterState,
} from '@/lib/storage/rankingViews';
import { cn } from '@/lib/utils/cn';

interface Chip {
  key: string;
  label: string;
  clear: () => RankingFilterState;
}

function buildChips(state: RankingFilterState): Chip[] {
  const chips: Chip[] = [];
  const [smin, smax] = state.scoreRange;
  if (smin !== SCORE_BOUNDS[0] || smax !== SCORE_BOUNDS[1]) {
    chips.push({
      key: 'score',
      label: `Score ${smin.toFixed(2)}–${smax.toFixed(2)}`,
      clear: () => ({ ...state, scoreRange: [...SCORE_BOUNDS] as [number, number] }),
    });
  }
  const [rmin, rmax] = state.rsRange;
  if (rmin !== RS_BOUNDS[0] || rmax !== RS_BOUNDS[1]) {
    chips.push({
      key: 'rs',
      label:
        rmax === RS_BOUNDS[1]
          ? `RS ${rmin}+`
          : rmin === RS_BOUNDS[0]
            ? `RS ≤ ${rmax}`
            : `RS ${rmin}–${rmax}`,
      clear: () => ({ ...state, rsRange: [...RS_BOUNDS] as [number, number] }),
    });
  }
  if (state.tiers.length > 0) {
    chips.push({
      key: 'tier',
      label: `Tier ${[...state.tiers].sort().join(', ')}`,
      clear: () => ({ ...state, tiers: [] }),
    });
  }
  if (state.sectors.length > 0) {
    const label =
      state.sectors.length <= 2
        ? state.sectors.join(', ')
        : `${state.sectors.slice(0, 2).join(', ')} +${state.sectors.length - 2}`;
    chips.push({
      key: 'sector',
      label,
      clear: () => ({ ...state, sectors: [] }),
    });
  }
  if (state.breakoutOnly) {
    chips.push({
      key: 'breakout',
      label: 'Breakout',
      clear: () => ({ ...state, breakoutOnly: false }),
    });
  }
  if (state.hasPatternOnly) {
    chips.push({
      key: 'pattern',
      label: 'Has pattern',
      clear: () => ({ ...state, hasPatternOnly: false }),
    });
  }
  return chips;
}

interface Props {
  state: RankingFilterState;
  onChange: (next: RankingFilterState) => void;
  matched: number;
  total: number;
}

export default function ActiveFilterChips({ state, onChange, matched, total }: Props) {
  const chips = buildChips(state);

  return (
    <div
      className={cn(
        'flex flex-wrap items-center gap-2 rounded-xl border border-slate-800 bg-slate-950/40 px-3 py-2',
      )}
    >
      <span className="text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-500">
        Active filters
      </span>
      {chips.length === 0 ? (
        <span className="text-[11px] text-slate-500">— none</span>
      ) : (
        chips.map((chip) => (
          <span
            key={chip.key}
            className="inline-flex items-center gap-1.5 rounded-full border border-slate-700 bg-slate-800/80 px-2.5 py-0.5 text-[11px] text-slate-200"
          >
            {chip.label}
            <button
              type="button"
              aria-label={`Clear ${chip.label}`}
              onClick={() => onChange(chip.clear())}
              className="text-slate-500 hover:text-rose-300"
            >
              ×
            </button>
          </span>
        ))
      )}
      {chips.length > 0 ? (
        <button
          type="button"
          onClick={() => onChange({ ...DEFAULT_FILTER_STATE })}
          className="text-[11px] text-rose-400 hover:text-rose-300"
        >
          Clear all
        </button>
      ) : null}
      <span className="ml-auto font-mono text-[11px] text-slate-400">
        {matched} of {total} match
      </span>
    </div>
  );
}
