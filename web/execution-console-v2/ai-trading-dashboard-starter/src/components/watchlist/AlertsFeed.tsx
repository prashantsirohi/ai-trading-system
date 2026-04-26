/**
 * Reverse-chronological alerts feed (Quantis proposal #02).
 *
 * Each row: severity dot · mono timestamp · rule name · symbol(s) ·
 * one-line reason · Mute / Snooze 1h / Delete overflow.
 */
import { cn } from '@/lib/utils/cn';
import type { AlertFiredEvent, AlertSeverity } from '@/lib/storage/watchlist';

const SEVERITY_DOT: Record<AlertSeverity, string> = {
  info: 'bg-blue-400',
  warn: 'bg-amber-400',
  critical: 'bg-rose-400',
};

const SEVERITY_LABEL: Record<AlertSeverity, string> = {
  info: 'INFO',
  warn: 'WARN',
  critical: 'CRIT',
};

const SEVERITY_PILL: Record<AlertSeverity, string> = {
  info: 'border-blue-700/60 bg-blue-500/10 text-blue-300',
  warn: 'border-amber-700/60 bg-amber-500/10 text-amber-300',
  critical: 'border-rose-700/60 bg-rose-500/15 text-rose-300',
};

function formatTs(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

interface Props {
  alerts: AlertFiredEvent[];
  onMute: (id: string) => void;
  onSnooze: (id: string) => void;
  onDelete: (id: string) => void;
}

export default function AlertsFeed({ alerts, onMute, onSnooze, onDelete }: Props) {
  const visible = alerts
    .filter((a) => !a.muted)
    .sort((a, b) => b.ts.localeCompare(a.ts));

  if (visible.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center rounded-2xl border border-slate-800 bg-slate-950/40 px-4 py-12 text-center">
        <p className="text-sm text-slate-400">No active alerts.</p>
        <p className="mt-1 text-xs text-slate-500">Fired events will appear here in real time.</p>
      </div>
    );
  }

  return (
    <ul className="flex flex-col gap-2">
      {visible.map((alert) => (
        <li
          key={alert.id}
          className={cn(
            'flex flex-col gap-2 rounded-xl border bg-slate-950/50 p-3',
            alert.severity === 'critical'
              ? 'border-rose-700/50'
              : alert.severity === 'warn'
                ? 'border-amber-700/40'
                : 'border-slate-800',
          )}
        >
          <div className="flex items-start gap-2.5">
            <span
              className={cn(
                'mt-1 h-2 w-2 shrink-0 rounded-full',
                SEVERITY_DOT[alert.severity],
              )}
            />
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-center gap-2">
                <span
                  className={cn(
                    'rounded border px-1.5 py-0.5 font-mono text-[9px] font-semibold uppercase tracking-wide',
                    SEVERITY_PILL[alert.severity],
                  )}
                >
                  {SEVERITY_LABEL[alert.severity]}
                </span>
                <span className="font-mono text-[10px] text-slate-500">
                  {formatTs(alert.ts)}
                </span>
                <span className="text-xs font-semibold text-slate-200">
                  {alert.ruleName}
                </span>
                {alert.symbols.map((s) => (
                  <span
                    key={s}
                    className="rounded border border-slate-700 px-1.5 py-0.5 font-mono text-[10px] text-slate-300"
                  >
                    {s}
                  </span>
                ))}
              </div>
              <p className="mt-1.5 text-[11px] leading-relaxed text-slate-400">
                {alert.reason}
              </p>
            </div>
          </div>
          <div className="ml-4 flex items-center gap-1.5">
            <button
              type="button"
              onClick={() => onMute(alert.id)}
              className="rounded border border-slate-700 px-2 py-0.5 text-[10px] text-slate-400 hover:border-slate-500 hover:text-slate-200"
            >
              Mute
            </button>
            <button
              type="button"
              onClick={() => onSnooze(alert.id)}
              className="rounded border border-slate-700 px-2 py-0.5 text-[10px] text-slate-400 hover:border-slate-500 hover:text-slate-200"
            >
              Snooze 1h
            </button>
            <button
              type="button"
              onClick={() => onDelete(alert.id)}
              className="rounded border border-slate-700 px-2 py-0.5 text-[10px] text-slate-400 hover:border-rose-500/50 hover:text-rose-300"
            >
              Delete
            </button>
          </div>
        </li>
      ))}
    </ul>
  );
}
