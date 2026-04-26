import { cn } from '@/lib/utils/cn';

export type CellState = 'agree' | 'disagree' | 'shadow-only';

interface Props {
  cells: CellState[];
}

const CELL_CLASS: Record<CellState, string> = {
  'agree':       'bg-emerald-700',
  'disagree':    'bg-rose-700',
  'shadow-only': 'bg-blue-700',
};

const LEGEND: { state: CellState; label: string; color: string }[] = [
  { state: 'agree',       label: 'Agree',       color: 'bg-emerald-700' },
  { state: 'disagree',    label: 'Disagree',    color: 'bg-rose-700' },
  { state: 'shadow-only', label: 'Shadow-only', color: 'bg-blue-700' },
];

export function buildAgreementCells(agreePct: number, total = 50): CellState[] {
  const agreeCount   = Math.round((agreePct / 100) * total);
  const remainder    = total - agreeCount;
  const shadowCount  = Math.round(remainder * 0.55);
  const disagreeCount = remainder - shadowCount;

  // Use a deterministic seeded pattern (not random) for stable renders.
  const cells: CellState[] = new Array(total).fill('agree');
  const seedPositions = [3, 6, 12, 17, 21, 25, 30, 34, 38, 46];
  const states: CellState[] = [
    ...new Array(disagreeCount).fill('disagree'),
    ...new Array(shadowCount).fill('shadow-only'),
  ];
  for (let i = 0; i < states.length; i++) {
    cells[seedPositions[i % seedPositions.length] + i] = states[i];
  }
  return cells;
}

export default function AgreementMatrix({ cells }: Props) {
  const counts = cells.reduce(
    (acc, s) => { acc[s]++; return acc; },
    { agree: 0, disagree: 0, 'shadow-only': 0 } as Record<CellState, number>,
  );

  return (
    <div>
      <div className="grid grid-cols-10 gap-1">
        {cells.map((state, i) => (
          <div
            key={i}
            className={cn('h-4 w-full rounded-[3px]', CELL_CLASS[state])}
          />
        ))}
      </div>
      <div className="mt-2.5 flex flex-wrap gap-3">
        {LEGEND.map(({ state, label, color }) => (
          <span key={state} className="flex items-center gap-1.5 text-[10px] text-slate-500">
            <span className={cn('inline-block h-2 w-2 rounded-sm', color)} />
            {label} ({counts[state]})
          </span>
        ))}
      </div>
    </div>
  );
}
