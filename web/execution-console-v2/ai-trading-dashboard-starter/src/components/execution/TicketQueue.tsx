import { cn } from '@/lib/utils/cn';
import type { DerivedOrder } from './derive';

export type TicketStatus = 'ready' | 'cap-hit' | 'sent';
export type TicketSide = 'buy' | 'sell';
export type BrokerRoute = 'DMA · limit' | 'VWAP / 30m' | 'IS / 45m' | 'VWAP / 60m' | 'DMA · market';

export interface Ticket {
  id: string;
  side: TicketSide;
  symbol: string;
  price: number;
  sector: string;
  tier: string;
  qty: number;
  route: BrokerRoute;
  status: TicketStatus;
  sentAt?: string;
}

const ROUTES: BrokerRoute[] = ['VWAP / 30m', 'DMA · limit', 'IS / 45m', 'DMA · market', 'VWAP / 60m'];

export function ordersToTickets(orders: DerivedOrder[]): Ticket[] {
  return orders.slice(0, 5).map((o, i) => {
    const qty = o.sizePct > 0 && o.entry > 0
      ? Math.round((o.sizePct / 100) * 1_00_00_000 / o.entry)
      : 100;
    const status: TicketStatus = o.sizePct > 5 ? 'cap-hit' : 'ready';
    return {
      id: o.symbol,
      side: 'buy',
      symbol: o.symbol,
      price: o.entry,
      sector: o.sector,
      tier: o.tier,
      qty,
      route: ROUTES[i % ROUTES.length],
      status,
    };
  });
}

const STATUS_PILL: Record<TicketStatus, string> = {
  'ready':   'border-emerald-700/50 bg-emerald-500/10 text-emerald-300',
  'cap-hit': 'border-amber-700/50 bg-amber-500/10 text-amber-300',
  'sent':    'border-slate-700 bg-slate-800/60 text-slate-400',
};

const STATUS_LABEL: Record<TicketStatus, string> = {
  'ready':   'Ready',
  'cap-hit': 'Cap hit',
  'sent':    'Sent',
};

interface Props {
  tickets: Ticket[];
  onSendAll: () => void;
  allChecksGreen: boolean;
}

export default function TicketQueue({ tickets, onSendAll, allChecksGreen }: Props) {
  const staged = tickets.filter((t) => t.status !== 'sent');
  const awaiting = staged.filter((t) => t.status === 'ready').length;

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-slate-200">
          Staged Tickets · {tickets.length}
        </h3>
        <div className="flex items-center gap-2">
          <span className="rounded-full border border-slate-700 bg-slate-800/60 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400">
            Hold
          </span>
          {awaiting > 0 && (
            <span className="rounded-full border border-amber-700/50 bg-amber-500/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider text-amber-300">
              {awaiting} awaiting
            </span>
          )}
          <button
            type="button"
            disabled={!allChecksGreen}
            onClick={onSendAll}
            className={cn(
              'rounded-xl px-3 py-1.5 text-[11px] font-semibold transition',
              allChecksGreen
                ? 'bg-blue-600 text-white hover:bg-blue-500'
                : 'cursor-not-allowed bg-slate-800 text-slate-600',
            )}
          >
            Send all (green)
          </button>
        </div>
      </div>

      <div className="space-y-2">
        {tickets.map((ticket) => (
          <div
            key={ticket.id}
            className={cn(
              'grid grid-cols-[auto_1fr_auto_auto_auto] items-center gap-3 rounded-xl border px-3 py-2.5 text-xs transition',
              ticket.status === 'sent'
                ? 'border-slate-800/50 bg-slate-950/30 opacity-60'
                : 'border-amber-700/40 bg-slate-950/50',
            )}
          >
            {/* Side */}
            <span
              className={cn(
                'rounded-md px-2 py-0.5 text-[10px] font-bold uppercase',
                ticket.side === 'buy'
                  ? 'bg-emerald-500/20 text-emerald-300'
                  : 'bg-rose-500/20 text-rose-300',
              )}
            >
              {ticket.side}
            </span>

            {/* Symbol + meta */}
            <div className="min-w-0">
              <span className="font-semibold text-slate-100">{ticket.symbol}</span>
              <span className="ml-1.5 font-mono text-[10px] text-slate-500">
                {ticket.price.toFixed(2)} · {ticket.sector} · {ticket.tier}
                {ticket.sentAt && ` · ${ticket.sentAt}`}
              </span>
            </div>

            {/* Qty */}
            <span className="font-mono text-slate-300">
              {ticket.qty.toLocaleString('en-IN')} sh
            </span>

            {/* Route */}
            <span className="font-mono text-[10px] text-slate-500">{ticket.route}</span>

            {/* Status */}
            <span
              className={cn(
                'rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide',
                STATUS_PILL[ticket.status],
              )}
            >
              {ticket.status === 'sent' && ticket.sentAt
                ? `Sent ${ticket.sentAt}`
                : STATUS_LABEL[ticket.status]}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
