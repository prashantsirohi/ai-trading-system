/**
 * Data-quality drill-in modal.
 *
 * Renders the failed DQ rules for a run with severity grouping. The "Blocked"
 * flag is implied: any error-tier DQ result blocks downstream stages.
 *
 * Triggered from the DqSummary chip in the detail pane.
 */
import type { DqResults } from '@/lib/api/runs';
import { cn } from '@/lib/utils/cn';

interface Props {
  data: DqResults | null | undefined;
  isLoading: boolean;
  open: boolean;
  onClose: () => void;
}

function severityClasses(sev: string): string {
  const norm = sev.toLowerCase();
  if (norm === 'error') return 'border-rose-500/40 bg-rose-500/10 text-rose-200';
  if (norm === 'warn' || norm === 'warning') {
    return 'border-amber-500/40 bg-amber-500/10 text-amber-200';
  }
  return 'border-slate-700 bg-slate-900/60 text-slate-300';
}

export default function DqModal({ data, isLoading, open, onClose }: Props) {
  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 p-4">
      <div
        className="absolute inset-0"
        onClick={onClose}
        role="presentation"
        aria-hidden
      />
      <div className="relative z-10 max-h-[90vh] w-full max-w-3xl overflow-hidden rounded-3xl border border-slate-800 bg-slate-950 shadow-2xl">
        <header className="flex items-center justify-between border-b border-slate-800 px-5 py-3">
          <div>
            <h3 className="text-lg font-semibold text-slate-100">Data Quality Results</h3>
            <p className="text-xs text-slate-400">
              {data ? `${data.results.length} rules · ${data.totalFailed} failed · ${data.totalPassed} passed` : '—'}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-full border border-slate-700 px-3 py-1 text-xs font-semibold text-slate-300 hover:border-slate-500 hover:text-slate-100"
            aria-label="Close DQ modal"
          >
            Close ✕
          </button>
        </header>

        <div className="max-h-[70vh] overflow-y-auto p-5">
          {isLoading ? (
            <p className="text-sm text-slate-500">Loading DQ results…</p>
          ) : !data || !data.available ? (
            <p className="text-sm text-slate-500">
              Data-quality results are not available for this run.
            </p>
          ) : data.results.length === 0 ? (
            <p className="text-sm text-emerald-300/80">
              No DQ records — either the rules haven't run, or all checks passed silently.
            </p>
          ) : (
            <div className="space-y-3">
              {Object.entries(data.countsBySeverity).map(([sev, counts]) => (
                <div
                  key={sev}
                  className={cn(
                    'flex items-center justify-between rounded-xl border px-3 py-2 text-sm',
                    severityClasses(sev),
                  )}
                >
                  <span className="font-semibold uppercase tracking-wider">{sev}</span>
                  <span className="tabular-nums">
                    {counts.failed} failed · {counts.passed} passed
                  </span>
                </div>
              ))}

              <div className="overflow-hidden rounded-2xl border border-slate-800">
                <table className="w-full text-sm">
                  <thead className="bg-slate-900/80 text-[10px] uppercase tracking-widest text-slate-500">
                    <tr>
                      <th className="px-3 py-2 text-left">Severity</th>
                      <th className="px-3 py-2 text-left">Stage</th>
                      <th className="px-3 py-2 text-left">Rule</th>
                      <th className="px-3 py-2 text-left">Status</th>
                      <th className="px-3 py-2 text-right">Failed</th>
                      <th className="px-3 py-2 text-left">Message</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800">
                    {data.results.map((r) => (
                      <tr key={r.resultId} className="text-slate-300">
                        <td className="px-3 py-2">
                          <span
                            className={cn(
                              'rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
                              severityClasses(r.severity),
                            )}
                          >
                            {r.severity}
                          </span>
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">{r.stageName}</td>
                        <td className="px-3 py-2 font-mono text-xs">{r.ruleId}</td>
                        <td className="px-3 py-2 text-xs">{r.status}</td>
                        <td className="px-3 py-2 text-right tabular-nums">
                          {r.failedCount.toLocaleString()}
                        </td>
                        <td className="px-3 py-2 text-xs text-slate-400">{r.message ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
