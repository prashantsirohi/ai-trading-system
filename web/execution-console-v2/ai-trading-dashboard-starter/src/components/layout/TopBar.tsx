import { useState } from 'react';
import { titleCase } from '@/lib/utils/text';
import { CommandIcon } from '@/components/control-tower/icons';
import { usePipelineWorkspace, useRefreshAll } from '@/lib/queries';
import { useTimeMachine } from '@/lib/context/TimeMachineContext';
import { cn } from '@/lib/utils/cn';
import PresenceAvatars from './PresenceAvatars';
import AuditLogDrawer from './AuditLogDrawer';
import TimeMachineBar from './TimeMachineBar';

interface Props {
  onOpenCommandBar: () => void;
}

const TRUST_PILL_TONE: Record<string, string> = {
  trusted: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300',
  live:    'border-emerald-500/40 bg-emerald-500/10 text-emerald-300',
  degraded:'border-amber-500/40 bg-amber-500/10 text-amber-300',
  legacy:  'border-amber-500/40 bg-amber-500/10 text-amber-300',
  warn:    'border-amber-500/40 bg-amber-500/10 text-amber-300',
  failed:  'border-rose-500/40 bg-rose-500/10 text-rose-300',
  blocked: 'border-rose-500/40 bg-rose-500/10 text-rose-300',
};

export default function TopBar({ onOpenCommandBar }: Props) {
  const refreshAll = useRefreshAll();
  const { data } = usePipelineWorkspace();
  const { isTimeTraveling } = useTimeMachine();

  const [auditOpen, setAuditOpen] = useState(false);
  const [tmOpen, setTmOpen] = useState(false);

  const trustKey = (data?.trust ?? '').toLowerCase();
  const pillClass = TRUST_PILL_TONE[trustKey] ?? 'border-slate-700 bg-slate-900 text-slate-300';
  const summaryText = data ? `${data.runId} · ${data.date}` : 'Live workspace status';

  return (
    <>
      <header className="flex h-16 items-center justify-between gap-4 border-b border-slate-800 bg-slate-950 px-4 md:px-6">
        {/* Left: trust pill + run ID */}
        <div className="flex min-w-0 items-center gap-3">
          {data ? (
            <span
              className={cn(
                'inline-flex shrink-0 items-center rounded-full border px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-widest',
                pillClass,
              )}
              title={`Trust: ${titleCase(data.trust)}`}
            >
              {titleCase(data.trust)}
            </span>
          ) : null}
          <span className="truncate text-sm text-slate-400">{summaryText}</span>
        </div>

        {/* Centre: operator presence avatars */}
        <div className="hidden md:flex">
          <PresenceAvatars />
        </div>

        {/* Right: actions */}
        <div className="flex shrink-0 items-center gap-2">
          {/* Time machine toggle */}
          <button
            type="button"
            onClick={() => setTmOpen((v) => !v)}
            title="Time machine — scrub to any prior run"
            className={cn(
              'rounded-xl border px-2.5 py-1.5 text-[11px] font-semibold transition',
              isTimeTraveling
                ? 'border-amber-600/60 bg-amber-500/15 text-amber-300'
                : tmOpen
                ? 'border-blue-500/40 bg-blue-500/10 text-blue-300'
                : 'border-slate-700 bg-slate-900 text-slate-400 hover:text-white',
            )}
          >
            ⏱
          </button>

          {/* Audit log toggle */}
          <button
            type="button"
            onClick={() => setAuditOpen(true)}
            title="Open audit log"
            className="rounded-xl border border-slate-700 bg-slate-900 px-2.5 py-1.5 text-[11px] font-semibold text-slate-400 hover:text-white transition"
          >
            📋
          </button>

          <button
            type="button"
            onClick={onOpenCommandBar}
            className={cn(
              'inline-flex items-center gap-1.5 rounded-2xl border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs',
              'text-slate-300 hover:bg-slate-800 hover:text-white',
            )}
            aria-label="Open command palette (Cmd+K or /)"
          >
            <CommandIcon size={14} />
            <span className="hidden md:inline">Command</span>
            <kbd className="rounded border border-slate-700 bg-slate-950 px-1 py-0.5 text-[10px] font-mono text-slate-400">
              ⌘K
            </kbd>
          </button>

          <button
            type="button"
            onClick={() => { void refreshAll(); }}
            className="rounded-2xl border border-slate-700 bg-slate-900 px-4 py-2 text-sm text-slate-100 hover:bg-slate-800"
          >
            Refresh
          </button>

          <button
            type="button"
            disabled
            className="rounded-2xl bg-blue-600/80 px-4 py-2 text-sm font-medium text-white opacity-70"
            title="Retry publish wiring is not enabled in this React console yet."
          >
            Retry Publish
          </button>
        </div>
      </header>

      {/* Collapsible time machine slider row */}
      <TimeMachineBar isOpen={tmOpen} />

      {/* Audit log side drawer */}
      <AuditLogDrawer isOpen={auditOpen} onClose={() => setAuditOpen(false)} />
    </>
  );
}
