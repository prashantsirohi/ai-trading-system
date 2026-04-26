/**
 * Alert rule editor drawer (Quantis proposal #02).
 *
 * Slides in from the right when the operator clicks "Rules" on a watchlist
 * row. Shows existing rules for the symbol and a form to add new ones.
 */
import { useState } from 'react';
import { cn } from '@/lib/utils/cn';
import {
  makeRule,
  type AlertChannel,
  type AlertCooldown,
  type AlertKind,
  type AlertRule,
  type AlertSeverity,
  type WatchlistEntry,
} from '@/lib/storage/watchlist';

const KIND_OPTIONS: { value: AlertKind; label: string; desc: string }[] = [
  { value: 'score', label: 'SCORE', desc: 'Crosses threshold up/down' },
  { value: 'breakout', label: 'BREAKOUT', desc: 'Confirmed on close' },
  { value: 'pattern', label: 'PATTERN', desc: 'New pattern recognized' },
  { value: 'pipeline', label: 'PIPELINE', desc: 'Stage failed / degraded trust' },
  { value: 'drawdown', label: 'DRAWDOWN', desc: 'Live DD exceeds limit' },
];

const SEVERITY_OPTIONS: AlertSeverity[] = ['info', 'warn', 'critical'];
const COOLDOWN_OPTIONS: AlertCooldown[] = ['5m', '1h', '1d', 'never'];
const CHANNEL_OPTIONS: { value: AlertChannel; label: string }[] = [
  { value: 'in-app', label: 'In-app (always)' },
  { value: 'slack', label: 'Slack #quantis-alerts' },
  { value: 'email', label: 'Email' },
];

const SEVERITY_RING: Record<AlertSeverity, string> = {
  info: 'border-blue-500/50 bg-blue-500/10 text-blue-200',
  warn: 'border-amber-500/50 bg-amber-500/10 text-amber-200',
  critical: 'border-rose-500/50 bg-rose-500/15 text-rose-200',
};

interface Props {
  entry: WatchlistEntry | null;
  onClose: () => void;
  onUpdateEntry: (updated: WatchlistEntry) => void;
}

export default function AlertRuleDrawer({ entry, onClose, onUpdateEntry }: Props) {
  const [kind, setKind] = useState<AlertKind>('score');
  const [threshold, setThreshold] = useState('7.50');
  const [severity, setSeverity] = useState<AlertSeverity>('warn');
  const [cooldown, setCooldown] = useState<AlertCooldown>('1h');
  const [channels, setChannels] = useState<AlertChannel[]>(['in-app']);

  if (!entry) return null;

  const toggleChannel = (ch: AlertChannel) => {
    if (ch === 'in-app') return; // always on
    setChannels((prev) =>
      prev.includes(ch) ? prev.filter((c) => c !== ch) : [...prev, ch],
    );
  };

  const handleAdd = () => {
    const thresholdNum =
      (kind === 'score' || kind === 'drawdown') && threshold.trim()
        ? parseFloat(threshold)
        : undefined;
    const rule = makeRule(kind, thresholdNum);
    rule.severity = severity;
    rule.cooldown = cooldown;
    rule.channels = channels;
    onUpdateEntry({ ...entry, rules: [...entry.rules, rule] });
  };

  const handleDeleteRule = (id: string) => {
    onUpdateEntry({ ...entry, rules: entry.rules.filter((r) => r.id !== id) });
  };

  const handleToggleRule = (id: string) => {
    onUpdateEntry({
      ...entry,
      rules: entry.rules.map((r) => (r.id === id ? { ...r, enabled: !r.enabled } : r)),
    });
  };

  const conditionPreview = (() => {
    const th = parseFloat(threshold);
    switch (kind) {
      case 'score':
        return `when ${entry.symbol}.score > ${Number.isNaN(th) ? '?' : th.toFixed(2)}`;
      case 'breakout':
        return `when ${entry.symbol}.breakout == true`;
      case 'pattern':
        return `when ${entry.symbol}.pattern != null`;
      case 'pipeline':
        return `when pipeline.status in ['failed', 'degraded']`;
      case 'drawdown':
        return `when ${entry.symbol}.drawdown > ${Number.isNaN(th) ? '?' : th.toFixed(1)}%`;
    }
  })();

  return (
    <div className="fixed inset-0 z-50">
      <button
        type="button"
        aria-label="Close alert rules drawer"
        className="absolute inset-0 bg-slate-950/70"
        onClick={onClose}
      />
      <aside className="absolute right-0 top-0 flex h-full w-full max-w-md flex-col overflow-y-auto border-l border-slate-800 bg-slate-900 shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between gap-4 border-b border-slate-800 p-4">
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-500">
              Alert Rules
            </p>
            <h2 className="text-lg font-semibold text-white">{entry.symbol}</h2>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg border border-slate-700 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800"
          >
            Close
          </button>
        </div>

        <div className="flex-1 space-y-5 p-4">
          {/* Existing rules */}
          <section>
            <h3 className="mb-2 text-[11px] font-semibold uppercase tracking-[0.1em] text-slate-500">
              Active Rules ({entry.rules.length})
            </h3>
            {entry.rules.length === 0 ? (
              <p className="text-xs text-slate-500">No rules yet — add one below.</p>
            ) : (
              <ul className="space-y-2">
                {entry.rules.map((rule) => (
                  <li
                    key={rule.id}
                    className="flex items-center justify-between gap-3 rounded-xl border border-slate-800 bg-slate-950/50 px-3 py-2.5"
                  >
                    <div className="min-w-0">
                      <div className="flex items-center gap-1.5">
                        <span
                          className={cn(
                            'rounded border px-1.5 py-0.5 font-mono text-[9px] font-bold uppercase',
                            SEVERITY_RING[rule.severity],
                          )}
                        >
                          {rule.severity}
                        </span>
                        <span className="text-xs font-medium text-slate-200">{rule.label}</span>
                      </div>
                      <p className="mt-0.5 font-mono text-[10px] text-slate-500">
                        cooldown {rule.cooldown} · {rule.channels.join(', ')}
                      </p>
                    </div>
                    <div className="flex shrink-0 items-center gap-1.5">
                      <button
                        type="button"
                        onClick={() => handleToggleRule(rule.id)}
                        className={cn(
                          'rounded border px-2 py-0.5 text-[10px] transition-colors',
                          rule.enabled
                            ? 'border-emerald-700/60 bg-emerald-500/10 text-emerald-300'
                            : 'border-slate-700 text-slate-500',
                        )}
                      >
                        {rule.enabled ? 'On' : 'Off'}
                      </button>
                      <button
                        type="button"
                        onClick={() => handleDeleteRule(rule.id)}
                        aria-label={`Delete rule ${rule.label}`}
                        className="rounded border border-slate-700 px-2 py-0.5 text-[10px] text-slate-500 hover:border-rose-500/50 hover:text-rose-300"
                      >
                        ×
                      </button>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>

          {/* Add rule form */}
          <section className="rounded-2xl border border-slate-800 bg-slate-950/40 p-4">
            <h3 className="mb-3 text-[11px] font-semibold uppercase tracking-[0.1em] text-slate-500">
              Add Rule
            </h3>

            {/* Kind */}
            <label className="mb-3 block">
              <span className="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                Trigger
              </span>
              <div className="grid grid-cols-1 gap-1">
                {KIND_OPTIONS.map((opt) => (
                  <button
                    key={opt.value}
                    type="button"
                    onClick={() => setKind(opt.value)}
                    className={cn(
                      'flex items-center justify-between rounded-lg border px-3 py-2 text-left text-xs transition-colors',
                      kind === opt.value
                        ? 'border-blue-500/40 bg-blue-500/10 text-white'
                        : 'border-slate-800 text-slate-400 hover:border-slate-600',
                    )}
                  >
                    <span className="font-mono font-bold">{opt.label}</span>
                    <span className="text-slate-500">{opt.desc}</span>
                  </button>
                ))}
              </div>
            </label>

            {/* Threshold (score / drawdown) */}
            {(kind === 'score' || kind === 'drawdown') && (
              <label className="mb-3 block">
                <span className="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                  Threshold {kind === 'drawdown' ? '(%)' : '(0–10)'}
                </span>
                <input
                  type="number"
                  step={kind === 'score' ? 0.1 : 1}
                  min={0}
                  max={kind === 'score' ? 10 : 100}
                  value={threshold}
                  onChange={(e) => setThreshold(e.target.value)}
                  className="w-full rounded-lg border border-slate-700 bg-slate-950/60 px-3 py-2 font-mono text-sm text-slate-200 focus:border-blue-500/60 focus:outline-none"
                />
              </label>
            )}

            {/* Condition preview */}
            <div className="mb-3 rounded-lg border border-dashed border-slate-700 bg-slate-950/40 px-3 py-2 font-mono text-[11px] leading-6 text-slate-400">
              <span className="text-slate-600">when </span>
              {conditionPreview.replace('when ', '')}
              <br />
              <span className="text-slate-600">cooldown </span>
              {cooldown}
              <br />
              <span className="text-slate-600">notify </span>
              {channels.join(', ')}
            </div>

            {/* Severity */}
            <label className="mb-3 block">
              <span className="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                Severity
              </span>
              <div className="flex gap-2">
                {SEVERITY_OPTIONS.map((s) => (
                  <button
                    key={s}
                    type="button"
                    onClick={() => setSeverity(s)}
                    className={cn(
                      'flex-1 rounded-lg border py-1.5 text-xs font-semibold uppercase tracking-wide transition-colors',
                      severity === s
                        ? SEVERITY_RING[s]
                        : 'border-slate-700 text-slate-500 hover:border-slate-600',
                    )}
                  >
                    {s}
                  </button>
                ))}
              </div>
            </label>

            {/* Cooldown */}
            <label className="mb-3 block">
              <span className="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                Cooldown
              </span>
              <div className="flex gap-2">
                {COOLDOWN_OPTIONS.map((c) => (
                  <button
                    key={c}
                    type="button"
                    onClick={() => setCooldown(c)}
                    className={cn(
                      'flex-1 rounded-lg border py-1.5 text-xs font-semibold uppercase tracking-wide transition-colors',
                      cooldown === c
                        ? 'border-blue-500/50 bg-blue-500/10 text-blue-200'
                        : 'border-slate-700 text-slate-500 hover:border-slate-600',
                    )}
                  >
                    {c}
                  </button>
                ))}
              </div>
            </label>

            {/* Channels */}
            <label className="mb-4 block">
              <span className="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                Channels
              </span>
              <div className="flex flex-col gap-1.5">
                {CHANNEL_OPTIONS.map((opt) => {
                  const active = channels.includes(opt.value);
                  const isAlways = opt.value === 'in-app';
                  return (
                    <button
                      key={opt.value}
                      type="button"
                      disabled={isAlways}
                      onClick={() => toggleChannel(opt.value)}
                      className={cn(
                        'flex items-center gap-2 rounded-lg border px-3 py-2 text-left text-xs transition-colors',
                        active
                          ? 'border-emerald-700/60 bg-emerald-500/10 text-emerald-200'
                          : 'border-slate-800 text-slate-400 hover:border-slate-600',
                        isAlways ? 'cursor-default opacity-70' : '',
                      )}
                    >
                      <span
                        className={cn(
                          'h-3 w-3 rounded-sm border',
                          active ? 'border-emerald-500 bg-emerald-500' : 'border-slate-600',
                        )}
                      />
                      {opt.label}
                    </button>
                  );
                })}
              </div>
            </label>

            <button
              type="button"
              onClick={handleAdd}
              className="w-full rounded-xl border border-blue-500/40 bg-blue-500/15 py-2.5 text-sm font-semibold text-blue-200 hover:border-blue-500/60 hover:bg-blue-500/20"
            >
              Add rule
            </button>
          </section>
        </div>
      </aside>
    </div>
  );
}
