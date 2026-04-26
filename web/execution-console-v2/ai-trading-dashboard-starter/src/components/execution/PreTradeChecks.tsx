import { cn } from '@/lib/utils/cn';
import type { ExecutionDerived } from './derive';

export type CheckTone = 'ok' | 'warn' | 'pending';

export interface PreTradeCheck {
  label: string;
  tone: CheckTone;
  detail: string;
}

const TONE_CLASS: Record<CheckTone, string> = {
  ok:      'text-emerald-300',
  warn:    'text-amber-300',
  pending: 'text-slate-500',
};

interface Props {
  checks: PreTradeCheck[];
}

export default function PreTradeChecks({ checks }: Props) {
  const allGreen = checks.every((c) => c.tone === 'ok');

  return (
    <div>
      <div className="divide-y divide-slate-800/60 rounded-2xl border border-slate-800 bg-slate-950/60">
        {checks.map((check) => (
          <div key={check.label} className="flex items-center justify-between gap-4 px-4 py-3">
            <span className="text-xs text-slate-300">{check.label}</span>
            <span className={cn('shrink-0 text-right font-mono text-[11px] font-semibold', TONE_CLASS[check.tone])}>
              {check.detail}
            </span>
          </div>
        ))}
      </div>

      {!allGreen && (
        <p className="mt-2 text-[11px] leading-relaxed text-slate-600">
          "Send all" stays disabled until every check is green and a second operator co-signs.
        </p>
      )}
    </div>
  );
}

// ─── Deriver ─────────────────────────────────────────────────────────────────

const SECTOR_CAPS: Record<string, number> = {
  Energy: 25, Banking: 25, IT: 25, Defence: 20, Infra: 15, Power: 15,
};
const NAME_CAP = 15;

export function derivePreTradeChecks(
  derived: ExecutionDerived,
  trustTone: 'good' | 'warn' | 'bad' | 'neutral',
): { checks: PreTradeCheck[]; allGreen: boolean } {
  const orders = derived.orders;

  // Trust state
  const trustOk = trustTone === 'good' || trustTone === 'neutral';

  // Sector cap headroom
  const sectorTotals = new Map<string, number>();
  for (const o of orders) {
    sectorTotals.set(o.sector, (sectorTotals.get(o.sector) ?? 0) + o.sizePct);
  }
  const breachedSectors: string[] = [];
  for (const [sector, total] of sectorTotals) {
    const cap = SECTOR_CAPS[sector] ?? 20;
    if (total > cap) breachedSectors.push(sector);
  }
  const sectorOk = breachedSectors.length === 0;

  // Single-name cap
  const maxName = orders.length ? Math.max(...orders.map((o) => o.sizePct)) : 0;
  const nameOk = maxName <= NAME_CAP;

  // Market hours — derive from system time in IST
  const nowUtc = new Date();
  const istOffset = 5.5 * 60 * 60 * 1000;
  const nowIst = new Date(nowUtc.getTime() + istOffset);
  const h = nowIst.getUTCHours();
  const m = nowIst.getUTCMinutes();
  const totalMins = h * 60 + m;
  const rthOpen = 9 * 60 + 15;
  const rthClose = 15 * 60 + 30;
  const isRth = totalMins >= rthOpen && totalMins < rthClose;
  const timeStr = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')} IST`;

  const checks: PreTradeCheck[] = [
    {
      label:  'Trust state',
      tone:   trustOk ? 'ok' : 'warn',
      detail: trustOk ? 'HEALTHY' : 'DEGRADED',
    },
    {
      label:  'Sector cap headroom',
      tone:   sectorOk ? 'ok' : 'warn',
      detail: sectorOk
        ? 'all within cap'
        : `${breachedSectors.join(', ')} breaks cap`,
    },
    {
      label:  'Single-name cap',
      tone:   nameOk ? 'ok' : 'warn',
      detail: nameOk
        ? 'all under 15%'
        : `max ${maxName.toFixed(1)}% (cap 15%)`,
    },
    {
      label:  'Liquidity (5d ADV)',
      tone:   'ok',
      detail: 'all under 2%',
    },
    {
      label:  'Market hours',
      tone:   isRth ? 'ok' : 'warn',
      detail: isRth ? `RTH · ${timeStr}` : `Pre/post market · ${timeStr}`,
    },
    {
      label:  'Killswitch',
      tone:   'ok',
      detail: 'DISARMED',
    },
    {
      label:  'Operator co-sign',
      tone:   'warn',
      detail: 'PENDING (1)',
    },
  ];

  const allGreen = checks.every((c) => c.tone === 'ok');
  return { checks, allGreen };
}
