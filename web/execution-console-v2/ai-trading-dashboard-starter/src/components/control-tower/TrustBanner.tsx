/**
 * Trust banner — the wide pill rendered just below the Decision Summary
 * on the Control Tower. Combines:
 *
 *   * System trust badge ("SYSTEM TRUSTED" / "DEGRADED" / "FAILED").
 *   * Active operator-relevant counters (artifact count, ranked count).
 *   * Top-sector callout from the slim workspace summary.
 *
 * Tone is keyed off ``trust`` first, then health checks. The banner is
 * cosmetic — it never gates user action; the actual blocking happens in
 * the execution view's gate component.
 */
import { ShieldAlertIcon, ShieldCheckIcon } from './icons';
import type { WorkspaceSnapshot } from '@/lib/api/workspace';
import { cn } from '@/lib/utils/cn';

interface Props {
  snapshot: WorkspaceSnapshot | undefined;
  isLoading?: boolean;
}

type Tone = 'trusted' | 'degraded' | 'failed' | 'unknown';

function resolveTone(snapshot: WorkspaceSnapshot | undefined): Tone {
  if (!snapshot) return 'unknown';
  const trust = (snapshot.summary.dataTrustStatus ?? '').toLowerCase();
  if (trust === 'trusted' || trust === 'live') return 'trusted';
  if (trust === 'failed' || trust === 'blocked') return 'failed';
  if (trust === 'degraded' || trust === 'legacy' || trust === 'warn') return 'degraded';
  return snapshot.available ? 'trusted' : 'unknown';
}

const TONE_STYLES: Record<Tone, { container: string; pill: string; label: string; sub: string }> = {
  trusted: {
    container: 'border-emerald-500/30 bg-emerald-950/30',
    pill: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40',
    label: 'SYSTEM TRUSTED',
    sub: 'Automated execution active. Policy guards nominal.',
  },
  degraded: {
    container: 'border-amber-500/30 bg-amber-950/30',
    pill: 'bg-amber-500/20 text-amber-300 border-amber-500/40',
    label: 'DEGRADED',
    sub: 'Trust diminished — review DQ and last-publish state.',
  },
  failed: {
    container: 'border-rose-500/30 bg-rose-950/30',
    pill: 'bg-rose-500/20 text-rose-300 border-rose-500/40',
    label: 'BLOCKED',
    sub: 'Execution disabled. Inspect failed run before resuming.',
  },
  unknown: {
    container: 'border-slate-700 bg-slate-900/60',
    pill: 'bg-slate-800 text-slate-300 border-slate-700',
    label: 'NO RUN YET',
    sub: 'Pipeline has not produced a workspace snapshot.',
  },
};

export default function TrustBanner({ snapshot, isLoading }: Props) {
  const tone = resolveTone(snapshot);
  const styles = TONE_STYLES[tone];
  const Icon = tone === 'trusted' ? ShieldCheckIcon : ShieldAlertIcon;

  return (
    <div
      className={cn(
        'flex items-center justify-between gap-6 rounded-xl border p-4',
        styles.container,
      )}
    >
      <div className="flex items-center gap-4">
        <div
          className={cn(
            'flex h-12 w-12 items-center justify-center rounded-full border',
            styles.pill,
          )}
        >
          <Icon size={24} />
        </div>
        <div>
          <div
            className={cn(
              'flex items-center gap-2 rounded border px-2.5 py-0.5 text-xs font-bold uppercase tracking-widest',
              styles.pill,
              'inline-flex',
            )}
          >
            {styles.label}
            {tone === 'trusted' ? (
              <span className="relative flex h-2 w-2">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75" />
                <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-500" />
              </span>
            ) : null}
          </div>
          <p className="mt-2 text-sm text-slate-400">{styles.sub}</p>
        </div>
      </div>

      <dl className="hidden flex-wrap items-center gap-8 text-right md:flex">
        <Metric label="Top Sector" value={snapshot?.summary.topSector ?? '—'} />
        <Metric label="Ranked" value={String(snapshot?.counts.ranked ?? 0)} />
        <Metric label="Breakouts" value={String(snapshot?.counts.breakouts ?? 0)} />
        <Metric label="Patterns" value={String(snapshot?.counts.patterns ?? 0)} />
        {isLoading ? (
          <span className="text-xs uppercase tracking-widest text-slate-500">
            refreshing…
          </span>
        ) : null}
      </dl>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col items-end">
      <dt className="text-[10px] uppercase tracking-[0.16em] text-slate-500">{label}</dt>
      <dd className="text-xl font-bold tabular-nums text-slate-200">{value}</dd>
    </div>
  );
}
