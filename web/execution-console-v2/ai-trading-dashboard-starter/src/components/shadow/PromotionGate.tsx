import { useState } from 'react';
import { cn } from '@/lib/utils/cn';

export type GateState = 'pass' | 'review' | 'pending';

export interface GateCheck {
  label: string;
  state: GateState;
  detail: string;
}

interface Props {
  checks: GateCheck[];
}

const STATE_CLASS: Record<GateState, string> = {
  pass:   'text-emerald-300',
  review: 'text-amber-300',
  pending: 'text-slate-500',
};

const STATE_LABEL: Record<GateState, string> = {
  pass:    'PASS',
  review:  'REVIEW',
  pending: 'PENDING',
};

export default function PromotionGate({ checks }: Props) {
  const [notice, setNotice] = useState<string | null>(null);
  const allPass = checks.every((c) => c.state === 'pass');

  function flash(msg: string) {
    setNotice(msg);
    setTimeout(() => setNotice(null), 3500);
  }

  return (
    <div>
      <div className="divide-y divide-slate-800/60 rounded-2xl border border-slate-800 bg-slate-950/60">
        {checks.map((check) => (
          <div key={check.label} className="flex items-center justify-between gap-4 px-4 py-3">
            <span className="text-xs text-slate-300">{check.label}</span>
            <span className={cn('shrink-0 text-right font-mono text-[11px] font-semibold', STATE_CLASS[check.state])}>
              {STATE_LABEL[check.state]} · {check.detail}
            </span>
          </div>
        ))}
      </div>

      {notice && (
        <div className="mt-3 rounded-xl border border-amber-700/40 bg-amber-500/10 px-4 py-2 text-xs text-amber-300">
          {notice}
        </div>
      )}

      <div className="mt-3 flex gap-2">
        <button
          type="button"
          disabled={!allPass}
          onClick={() => flash('Promote action is not wired to the backend yet — connect the /api/shadow/promote endpoint to enable.')}
          className={cn(
            'flex-1 rounded-xl py-2.5 text-xs font-semibold transition',
            allPass
              ? 'bg-blue-600 text-white hover:bg-blue-500'
              : 'cursor-not-allowed bg-slate-800 text-slate-600',
          )}
        >
          Promote to prod
        </button>
        <button
          type="button"
          onClick={() => flash('Hold action is not wired yet — model stays in shadow evaluation.')}
          className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-2.5 text-xs font-semibold text-slate-300 hover:border-slate-500 hover:text-white transition"
        >
          Hold for review
        </button>
      </div>

      {!allPass && (
        <p className="mt-2 text-[11px] text-slate-600 leading-relaxed">
          "Promote" unlocks when all gate checks pass. {checks.filter((c) => c.state !== 'pass').length} check(s) require attention.
        </p>
      )}
    </div>
  );
}
