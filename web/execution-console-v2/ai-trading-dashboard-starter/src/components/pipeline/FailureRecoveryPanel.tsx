/**
 * Failure recovery panel (Quantis proposal #07).
 *
 * Shown only when the pipeline workspace reports a failed/terminated state.
 * Surfaces the failed stage + a tail of warnings as a stand-in for live
 * logs (no streaming-logs endpoint yet — `warnings[]` is the most
 * informative signal we have today).
 *
 * Retry / halt actions are visually present but unwired pending the
 * ``/pipeline/{run}/retry`` endpoint. The parent receives a notice so it
 * can flash a transient explainer.
 */
import { useMemo, useState } from 'react';

import { cn } from '@/lib/utils/cn';
import type { PipelineWorkspaceResponse } from '@/types/api';

type LogLevel = 'info' | 'warn' | 'err';

interface LogLine {
  ts: string;
  level: LogLevel;
  text: string;
}

function inferLevel(text: string): LogLevel {
  const lower = text.toLowerCase();
  if (
    lower.includes('error') ||
    lower.includes('failed') ||
    lower.includes('exception') ||
    lower.includes('timeout')
  )
    return 'err';
  if (
    lower.includes('warn') ||
    lower.includes('degrad') ||
    lower.includes('stale') ||
    lower.includes('retry')
  )
    return 'warn';
  return 'info';
}

function formatStartedAt(value: string | null): string {
  if (!value) return 'unknown time';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    month: 'short',
    day: '2-digit',
  });
}

function buildLogLines(workspace: PipelineWorkspaceResponse): LogLine[] {
  const startedAt = workspace.task?.startedAt ?? null;
  const baseTs = startedAt ?? new Date().toISOString();
  const seedTs = (() => {
    const parsed = new Date(baseTs);
    if (Number.isNaN(parsed.getTime())) return '--:--:--';
    return parsed.toLocaleTimeString(undefined, { hour12: false });
  })();

  const lines: LogLine[] = [];
  if (workspace.task) {
    lines.push({
      ts: seedTs,
      level: 'info',
      text: `stage=${workspace.task.currentStageLabel} status=${workspace.task.status}`,
    });
  }
  for (const warning of workspace.warnings) {
    lines.push({ ts: seedTs, level: inferLevel(warning), text: warning });
  }
  if (lines.length === 0) {
    lines.push({
      ts: seedTs,
      level: 'err',
      text: 'pipeline reported failure with no warning detail — check backend logs',
    });
  }
  return lines;
}

interface Props {
  workspace: PipelineWorkspaceResponse;
}

export default function FailureRecoveryPanel({ workspace }: Props) {
  const [notice, setNotice] = useState<string | null>(null);
  const logs = useMemo(() => buildLogLines(workspace), [workspace]);

  const stageLabel = workspace.task?.currentStageLabel ?? 'Pipeline';
  const failedAt = formatStartedAt(workspace.task?.startedAt ?? null);
  const errorReason =
    workspace.warnings.find((w) => inferLevel(w) === 'err') ??
    workspace.warnings[0] ??
    `${stageLabel} reported a failure`;

  const flashNotice = (msg: string) => {
    setNotice(msg);
    window.setTimeout(() => setNotice(null), 2500);
  };

  const Action = ({
    label,
    variant = 'secondary',
    onClick,
  }: {
    label: string;
    variant?: 'primary' | 'secondary' | 'danger';
    onClick: () => void;
  }) => (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        'rounded-lg px-3.5 py-2 text-xs font-semibold transition-colors',
        variant === 'primary'
          ? 'bg-blue-600/85 text-white hover:bg-blue-600'
          : variant === 'danger'
            ? 'border border-rose-700 bg-transparent text-rose-300 hover:border-rose-500/70 hover:bg-rose-500/10'
            : 'border border-slate-700 bg-slate-900 text-slate-200 hover:border-slate-500',
      )}
    >
      {label}
    </button>
  );

  return (
    <div className="grid gap-3 lg:grid-cols-[1.3fr_1fr]">
      {/* Stage failure card */}
      <div className="rounded-2xl border border-rose-700/60 bg-rose-950/20 p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-500">
              Stage · {stageLabel}
            </div>
            <div className="mt-1 text-sm font-semibold text-slate-100">
              Failed at {failedAt}
            </div>
            <div className="mt-1 break-words font-mono text-[11px] text-slate-400">
              {errorReason}
            </div>
          </div>
          <span className="shrink-0 rounded-full border border-rose-700 bg-rose-500/15 px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wider text-rose-300">
            Failed
          </span>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <Action
            label={`Retry ${stageLabel}`}
            variant="primary"
            onClick={() => flashNotice('Retry endpoint not wired yet — operator action logged locally.')}
          />
          <Action
            label="Retry from start"
            onClick={() => flashNotice('Retry-from-start not wired yet.')}
          />
          <Action
            label="Skip & Publish"
            onClick={() => flashNotice('Skip & Publish not wired yet.')}
          />
          <Action
            label="Halt run"
            variant="danger"
            onClick={() => flashNotice('Halt requires two-key approval — flow not wired yet.')}
          />
        </div>

        {notice ? (
          <div className="mt-3 rounded-md border border-amber-700/60 bg-amber-500/10 px-3 py-1.5 text-[11px] text-amber-200">
            {notice}
          </div>
        ) : null}
      </div>

      {/* Log tail */}
      <div className="flex min-w-0 flex-col">
        <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-500">
          Live log tail · {workspace.task?.label ?? 'pipeline'}.log
        </div>
        <div className="max-h-[200px] overflow-auto rounded-2xl border border-slate-800 bg-black p-3 font-mono text-[11px] leading-7">
          {logs.map((line, i) => (
            <div key={`${line.ts}-${i}`}>
              <span className="text-slate-600">{line.ts}</span>{' '}
              <span
                className={cn(
                  line.level === 'err'
                    ? 'text-rose-400'
                    : line.level === 'warn'
                      ? 'text-amber-300'
                      : 'text-slate-300',
                )}
              >
                {line.level === 'err'
                  ? 'ERR'
                  : line.level === 'warn'
                    ? 'WARN'
                    : 'INFO'}{' '}
                {line.text}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
