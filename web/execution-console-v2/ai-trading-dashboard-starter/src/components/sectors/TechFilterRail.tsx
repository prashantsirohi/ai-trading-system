import { cn } from '@/lib/utils/cn';

export type IndicatorKey =
  | 'aboveMa50' | 'aboveMa200' | 'goldenCross'
  | 'rsiInRange' | 'macdBullish' | 'adxAbove20'
  | 'bbSqueeze' | 'atrRising'
  | 'volExpand' | 'obvRising'
  | 'near52wHigh' | 'pivotTaken';

export interface IndicatorDef {
  key: IndicatorKey;
  label: string;
  cond: string;
}

export const INDICATOR_GROUPS: { heading: string; items: IndicatorDef[] }[] = [
  {
    heading: 'Trend',
    items: [
      { key: 'aboveMa50',   label: 'Above 50DMA',   cond: 'px>ma50'  },
      { key: 'aboveMa200',  label: 'Above 200DMA',  cond: 'px>ma200' },
      { key: 'goldenCross', label: 'Golden cross',  cond: '50>200'   },
    ],
  },
  {
    heading: 'Momentum',
    items: [
      { key: 'rsiInRange',  label: 'RSI(14) 40–70', cond: '40–70'    },
      { key: 'macdBullish', label: 'MACD bullish',  cond: 'sig>0'    },
      { key: 'adxAbove20',  label: 'ADX > 20',      cond: 'trending' },
    ],
  },
  {
    heading: 'Volatility',
    items: [
      { key: 'bbSqueeze', label: 'BB squeeze', cond: 'low σ' },
      { key: 'atrRising', label: 'ATR rising', cond: 'vol↑'  },
    ],
  },
  {
    heading: 'Volume',
    items: [
      { key: 'volExpand', label: 'Vol > 1.5× ADV', cond: 'expand' },
      { key: 'obvRising', label: 'OBV rising',      cond: 'accum'  },
    ],
  },
  {
    heading: 'Levels',
    items: [
      { key: 'near52wHigh', label: '52w high', cond: '≤2%' },
      { key: 'pivotTaken',  label: 'Pivot taken', cond: 'PP↑' },
    ],
  },
];

interface Props {
  active: Set<IndicatorKey>;
  onToggle: (key: IndicatorKey) => void;
}

export default function TechFilterRail({ active, onToggle }: Props) {
  return (
    <aside className="w-48 shrink-0 space-y-4 pr-2">
      {INDICATOR_GROUPS.map((group) => (
        <div key={group.heading}>
          <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-widest text-slate-500">
            {group.heading}
          </p>
          <div className="space-y-1">
            {group.items.map((item) => {
              const isOn = active.has(item.key);
              return (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => onToggle(item.key)}
                  className="flex w-full items-center justify-between gap-2 rounded-lg px-2 py-1.5 hover:bg-slate-800/60 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    <span
                      className={cn(
                        'h-3.5 w-3.5 shrink-0 rounded-[3px] border transition-colors',
                        isOn
                          ? 'border-emerald-500 bg-emerald-500'
                          : 'border-slate-600 bg-transparent',
                      )}
                    >
                      {isOn && (
                        <svg viewBox="0 0 12 12" fill="none" className="h-full w-full p-0.5">
                          <path d="M2 6 L5 9 L10 3" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                      )}
                    </span>
                    <span className={cn('text-xs', isOn ? 'text-slate-100' : 'text-slate-400')}>
                      {item.label}
                    </span>
                  </div>
                  <span className="font-mono text-[9px] text-slate-600">{item.cond}</span>
                </button>
              );
            })}
          </div>
        </div>
      ))}

      {active.size > 0 && (
        <button
          type="button"
          onClick={() => INDICATOR_GROUPS.flatMap((g) => g.items).forEach((i) => active.has(i.key) && onToggle(i.key))}
          className="w-full rounded-lg border border-rose-700/40 bg-rose-500/10 py-1.5 text-[10px] font-semibold text-rose-400 hover:bg-rose-500/20 transition-colors"
        >
          Clear all filters
        </button>
      )}
    </aside>
  );
}
