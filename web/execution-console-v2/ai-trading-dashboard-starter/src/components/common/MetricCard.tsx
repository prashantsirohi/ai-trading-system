import type { MetricCard as MetricCardType } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

const toneMap = {
  blue: 'text-blue-400',
  green: 'text-emerald-400',
  yellow: 'text-amber-400',
  purple: 'text-fuchsia-400',
};

export default function MetricCard({ label, value, tone = 'blue' }: MetricCardType) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow-soft">
      <div className="text-sm text-slate-400">{label}</div>
      <div className={cn('mt-2 text-3xl font-semibold', toneMap[tone])}>{value}</div>
    </div>
  );
}
