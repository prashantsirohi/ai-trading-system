/**
 * Floating "Open Compare" launcher.
 *
 * Visible whenever the WorkspaceContext has at least one symbol pinned.
 * Mirrors the Ranking-page ComparisonTray footer but lives at the AppLayout
 * level so the selection follows the user across pages.
 */
import { useWorkspace } from './WorkspaceContext';
import { cn } from '@/lib/utils/cn';

export default function CompareTrayLauncher() {
  const { compareSymbols, openCompare, toggleCompare, clearCompare, compareOpen, workspaceSymbol } =
    useWorkspace();

  // Hide while the modal is open or while the workspace covers the screen
  // — both already give the user direct access to compare actions.
  if (compareSymbols.length === 0 || compareOpen || workspaceSymbol) return null;

  return (
    <div className="pointer-events-none fixed inset-x-0 bottom-4 z-30 flex justify-center px-4">
      <div className="pointer-events-auto flex w-full max-w-3xl items-center gap-2 rounded-2xl border border-slate-700 bg-slate-950/90 px-4 py-3 shadow-2xl backdrop-blur">
        <span className="text-[10px] uppercase tracking-widest text-slate-400">
          Compare ({compareSymbols.length}/3)
        </span>
        <ul className="flex flex-wrap gap-1">
          {compareSymbols.map((s) => (
            <li key={s}>
              <button
                type="button"
                onClick={() => toggleCompare(s)}
                className="rounded-full border border-slate-700 bg-slate-900 px-2 py-0.5 text-[11px] font-mono text-slate-200 hover:border-rose-500/60 hover:text-rose-200"
                title="Remove from compare"
              >
                {s} ✕
              </button>
            </li>
          ))}
        </ul>
        <div className="ml-auto flex gap-2">
          <button
            type="button"
            onClick={clearCompare}
            className="rounded-full border border-slate-700 bg-slate-900/60 px-3 py-1 text-[11px] font-semibold uppercase tracking-wider text-slate-300 hover:border-slate-500"
          >
            Clear
          </button>
          <button
            type="button"
            onClick={openCompare}
            disabled={compareSymbols.length < 2}
            className={cn(
              'rounded-full border px-3 py-1 text-[11px] font-semibold uppercase tracking-wider transition-colors',
              compareSymbols.length >= 2
                ? 'border-blue-500/60 bg-blue-500/15 text-blue-100 hover:border-blue-300/80'
                : 'border-slate-800 bg-slate-900/60 text-slate-600',
            )}
          >
            Open Compare
          </button>
        </div>
      </div>
    </div>
  );
}
