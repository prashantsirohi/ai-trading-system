/**
 * Filter chip bar for the Ranking view.
 *
 * The filter set is fixed (matches the Canvas design) — clients pass the
 * active key + a search string and receive callbacks. Filtering itself is
 * applied by the parent on the row list so the chip bar stays presentational.
 */
import { cn } from '@/lib/utils/cn';

export type RankingFilter = 'all' | 'tier-a' | 'breakouts' | 'patterns';

interface ChipDef {
  key: RankingFilter;
  label: string;
  hint: string;
}

const CHIPS: ChipDef[] = [
  { key: 'all', label: 'All Tiers', hint: 'Every ranked symbol.' },
  { key: 'tier-a', label: 'Tier A', hint: 'Top-quality candidates only.' },
  { key: 'breakouts', label: 'Breakouts Only', hint: 'Symbols with confirmed breakout.' },
  { key: 'patterns', label: 'Patterns Active', hint: 'Symbols with an active chart pattern.' },
];

interface Props {
  active: RankingFilter;
  onChange: (next: RankingFilter) => void;
  search: string;
  onSearchChange: (next: string) => void;
  total: number;
  matched: number;
}

export default function FilterChipBar({
  active,
  onChange,
  search,
  onSearchChange,
  total,
  matched,
}: Props) {
  return (
    <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
      <div className="flex flex-wrap gap-2">
        {CHIPS.map((chip) => {
          const selected = chip.key === active;
          return (
            <button
              key={chip.key}
              type="button"
              onClick={() => onChange(chip.key)}
              title={chip.hint}
              className={cn(
                'rounded-full border px-3.5 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors',
                selected
                  ? 'border-blue-500/60 bg-blue-500/15 text-blue-200'
                  : 'border-slate-700 bg-slate-900/60 text-slate-300 hover:border-slate-500 hover:text-slate-100',
              )}
            >
              {chip.label}
            </button>
          );
        })}
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs uppercase tracking-widest text-slate-500">
          {matched} / {total}
        </span>
        <label className="relative">
          <span className="sr-only">Filter ranked symbols</span>
          <input
            type="search"
            value={search}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder="Search symbol or sector…"
            className="w-64 rounded-lg border border-slate-700 bg-slate-950/60 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-500 focus:border-blue-500/60 focus:outline-none"
          />
        </label>
      </div>
    </div>
  );
}
