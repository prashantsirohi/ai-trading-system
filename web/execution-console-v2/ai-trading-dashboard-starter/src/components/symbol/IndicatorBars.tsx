import { cn } from '@/lib/utils/cn';
import type { DerivedIndicators } from '@/lib/symbol/derive';

interface BarRowProps {
  label: string;
  value: string;
  /** 0–1 fill fraction */
  fill: number;
  /** Whether the bar is zero-centred (MACD, OBV) */
  centered?: boolean;
  tone: 'bull' | 'bear' | 'neutral';
}

const TONE_BAR: Record<BarRowProps['tone'], string> = {
  bull:    'bg-emerald-500',
  bear:    'bg-rose-500',
  neutral: 'bg-slate-500',
};

function BarRow({ label, value, fill, centered, tone }: BarRowProps) {
  return (
    <div className="grid grid-cols-[80px_1fr_44px] items-center gap-3">
      <span className="text-right text-xs text-slate-400">{label}</span>
      <div className="relative h-2 overflow-hidden rounded-full bg-slate-800">
        {centered ? (
          // Zero-centred bar: starts at 50%, extends left or right
          <div
            className={cn('absolute top-0 h-full rounded-full', TONE_BAR[tone])}
            style={{
              left:  fill >= 0 ? '50%' : `${50 + fill * 50}%`,
              width: `${Math.abs(fill) * 50}%`,
            }}
          />
        ) : (
          <div
            className={cn('h-full rounded-full', TONE_BAR[tone])}
            style={{ width: `${Math.max(2, fill * 100)}%` }}
          />
        )}
        {centered && <div className="absolute left-1/2 top-0 h-full w-px bg-slate-600" />}
      </div>
      <span className={cn('text-right font-mono text-[11px]', tone === 'bull' ? 'text-emerald-400' : tone === 'bear' ? 'text-rose-400' : 'text-slate-400')}>
        {value}
      </span>
    </div>
  );
}

interface Props {
  indicators: DerivedIndicators;
}

function rsiBull(rsi: number): BarRowProps['tone'] {
  return rsi > 70 ? 'bear' : rsi >= 40 ? 'bull' : 'neutral';
}

export default function IndicatorBars({ indicators }: Props) {
  const { rsi, macd, stochK, adx, atrPct, obvSlope } = indicators;

  return (
    <div className="space-y-2.5">
      <BarRow
        label="RSI(14)"
        value={String(rsi)}
        fill={rsi / 100}
        tone={rsiBull(rsi)}
      />
      <BarRow
        label="MACD"
        value={macd >= 0 ? `+${macd.toFixed(2)}` : macd.toFixed(2)}
        fill={macd}
        centered
        tone={macd > 0.05 ? 'bull' : macd < -0.05 ? 'bear' : 'neutral'}
      />
      <BarRow
        label="Stoch K"
        value={String(stochK)}
        fill={stochK / 100}
        tone={stochK > 80 ? 'neutral' : stochK >= 20 ? 'bull' : 'bear'}
      />
      <BarRow
        label="ADX(14)"
        value={String(adx)}
        fill={adx / 60}
        tone={adx >= 25 ? 'bull' : 'neutral'}
      />
      <BarRow
        label="ATR%"
        value={`${atrPct.toFixed(1)}%`}
        fill={atrPct / 5}
        tone="neutral"
      />
      <BarRow
        label="OBV slope"
        value={`+${obvSlope}`}
        fill={obvSlope === 'rising' ? 0.7 : obvSlope === 'flat' ? 0 : -0.7}
        centered
        tone={obvSlope === 'rising' ? 'bull' : obvSlope === 'falling' ? 'bear' : 'neutral'}
      />
    </div>
  );
}
