import { cn } from '@/lib/utils/cn';
import type { StockRow } from '@/types/dashboard';

const TONES: Record<StockRow['tier'], string> = {
  A: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200',
  B: 'border-amber-500/40 bg-amber-500/15 text-amber-200',
  C: 'border-rose-500/40 bg-rose-500/15 text-rose-200',
};

export default function TierBadge({ tier, className }: { tier: StockRow['tier']; className?: string }) {
  return (
    <span
      className={cn(
        'inline-flex h-6 w-6 items-center justify-center rounded-full border text-[11px] font-bold',
        TONES[tier],
        className,
      )}
      aria-label={`Tier ${tier}`}
    >
      {tier}
    </span>
  );
}
