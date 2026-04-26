/**
 * KPI strip at the top of the Runs audit view.
 *
 * Computes four headline metrics directly from the runs list:
 *   * Latest status — most recent run's status pill.
 *   * Last successful run — relative time since the most recent ``success``.
 *   * Failed runs (24h) — count of ``failed`` rows in the last 24 hours.
 *   * Publish errors (24h) — count of rows with ``error_class === 'publish'``
 *     (or message containing "publish") in the last 24 hours.
 *
 * Source of truth: ``GET /api/execution/runs?limit=`` payload normalised by
 * ``getRunsList``.
 */
import type { RunSummary } from '@/lib/api/runs';
import { cn } from '@/lib/utils/cn';

interface Props {
  runs: RunSummary[];
}

type Tone = 'good' | 'warn' | 'bad' | 'neutral';

const TONE_CLASSES: Record<Tone, string> = {
  good: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200',
  warn: 'border-amber-500/40 bg-amber-500/10 text-amber-200',
  bad: 'border-rose-500/40 bg-rose-500/10 text-rose-200',
  neutral: 'border-slate-700 bg-slate-900/60 text-slate-300',
};

function statusTone(status: string | null | undefined): Tone {
  const norm = (status ?? '').toLowerCase();
  if (norm === 'success' || norm === 'succeeded' || norm === 'completed') return 'good';
  if (norm === 'failed' || norm === 'error') return 'bad';
  if (norm === 'running' || norm === 'in_progress' || norm === 'partial') return 'warn';
  return 'neutral';
}

function relativeTime(iso: string | null): string {
  if (!iso) return '—';
  const ts = new Date(iso).getTime();
  if (!Number.isFinite(ts)) return iso;
  const ms = Date.now() - ts;
  const min = Math.floor(ms / 60_000);
  if (min < 1) return 'just now';
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const days = Math.floor(hr / 24);
  return `${days}d ago`;
}

function within24h(iso: string | null): boolean {
  if (!iso) return false;
  const ts = new Date(iso).getTime();
  if (!Number.isFinite(ts)) return false;
  return Date.now() - ts <= 24 * 60 * 60 * 1000;
}

export default function RunsKpiStrip({ runs }: Props) {
  const latest = runs[0];
  const lastSuccess = runs.find((r) => statusTone(r.status) === 'good');
  const failed24h = runs.filter(
    (r) => statusTone(r.status) === 'bad' && within24h(r.endedAt ?? r.startedAt),
  ).length;
  const publishErrors24h = runs.filter((r) => {
    if (!within24h(r.endedAt ?? r.startedAt)) return false;
    const cls = (r.errorClass ?? '').toLowerCase();
    const msg = (r.errorMessage ?? '').toLowerCase();
    return cls.includes('publish') || msg.includes('publish');
  }).length;

  const tiles: Array<{ label: string; value: string; sub?: string; tone: Tone }> = [
    {
      label: 'Latest status',
      value: latest?.status ? latest.status.toUpperCase() : '—',
      sub: latest ? `${latest.runId.slice(0, 12)} · ${relativeTime(latest.startedAt)}` : undefined,
      tone: statusTone(latest?.status),
    },
    {
      label: 'Last successful run',
      value: lastSuccess ? relativeTime(lastSuccess.endedAt ?? lastSuccess.startedAt) : 'never',
      sub: lastSuccess?.runId.slice(0, 12),
      tone: lastSuccess ? 'good' : 'bad',
    },
    {
      label: 'Failed runs · 24h',
      value: String(failed24h),
      tone: failed24h === 0 ? 'good' : failed24h <= 2 ? 'warn' : 'bad',
    },
    {
      label: 'Publish errors · 24h',
      value: String(publishErrors24h),
      tone: publishErrors24h === 0 ? 'good' : publishErrors24h <= 1 ? 'warn' : 'bad',
    },
  ];

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {tiles.map((tile) => (
        <div
          key={tile.label}
          className={cn(
            'rounded-2xl border p-4 transition-colors',
            TONE_CLASSES[tile.tone],
          )}
        >
          <p className="text-[11px] font-semibold uppercase tracking-widest opacity-80">
            {tile.label}
          </p>
          <p className="mt-2 text-xl font-semibold tabular-nums">{tile.value}</p>
          {tile.sub ? (
            <p className="mt-1 text-[11px] uppercase tracking-wider opacity-70">{tile.sub}</p>
          ) : null}
        </div>
      ))}
    </div>
  );
}
