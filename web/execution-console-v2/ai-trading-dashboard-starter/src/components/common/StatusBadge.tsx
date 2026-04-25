import { cn } from '@/lib/utils/cn';
import { titleCase } from '@/lib/utils/text';

export type StatusTone = 'good' | 'warn' | 'bad' | 'neutral';

const TONE_CLASSES: Record<StatusTone, string> = {
  good: 'border-emerald-700 bg-emerald-950/40 text-emerald-300',
  warn: 'border-amber-700 bg-amber-950/40 text-amber-300',
  bad: 'border-rose-700 bg-rose-950/40 text-rose-300',
  neutral: 'border-slate-700 bg-slate-900 text-slate-300',
};

const STATUS_TO_TONE: Record<string, StatusTone> = {
  // Good
  ok: 'good',
  healthy: 'good',
  trusted: 'good',
  completed: 'good',
  passed: 'good',
  // Warn
  warn: 'warn',
  degraded: 'warn',
  legacy: 'warn',
  running: 'warn',
  pending: 'warn',
  // Bad
  error: 'bad',
  failed: 'bad',
  blocked: 'bad',
  terminated: 'bad',
  completed_with_publish_errors: 'bad',
};

export function statusTone(status: string | null | undefined): StatusTone {
  if (!status) {
    return 'neutral';
  }
  return STATUS_TO_TONE[status.toLowerCase()] ?? 'neutral';
}

export interface StatusBadgeProps {
  /** Raw status string from the backend (e.g. "ok", "running", "failed"). */
  status: string | null | undefined;
  /** Optional override for the displayed label. Defaults to title-cased status. */
  label?: string;
  /** Force a tone instead of inferring from `status`. */
  tone?: StatusTone;
  className?: string;
}

export default function StatusBadge({
  status,
  label,
  tone,
  className,
}: StatusBadgeProps) {
  const resolvedTone = tone ?? statusTone(status);
  const display = label ?? titleCase(status);
  return (
    <span
      className={cn(
        'inline-flex rounded-full border px-2 py-1 text-xs font-medium',
        TONE_CLASSES[resolvedTone],
        className,
      )}
    >
      {display}
    </span>
  );
}
