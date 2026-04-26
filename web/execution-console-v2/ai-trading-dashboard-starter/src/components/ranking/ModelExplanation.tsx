/**
 * Model Explanation — three-up callout describing the strongest factor,
 * the active catalyst, and the weakest/limiting factor.
 *
 * Pulls everything it needs from the per-symbol ranking detail. When the
 * detail is unavailable it falls back to the row's StockRow attributes so
 * the panel never blanks out.
 */
import type { FactorBlock, RankingDetail } from '@/lib/api/ranking';
import type { StockRow } from '@/types/dashboard';

interface Props {
  detail: RankingDetail;
  row: StockRow;
}

function strongestFactor(blocks: FactorBlock[]): FactorBlock | null {
  const known = blocks.filter((b) => ['rs', 'volume', 'trend', 'sector'].includes(b.bucket));
  if (known.length === 0) return null;
  return known.reduce((best, current) => (current.value > best.value ? current : best));
}

function weakestFactor(blocks: FactorBlock[]): FactorBlock | null {
  const known = blocks.filter((b) => ['rs', 'volume', 'trend', 'sector'].includes(b.bucket));
  if (known.length === 0) return null;
  return known.reduce((worst, current) => (current.value < worst.value ? current : worst));
}

const BUCKET_LABEL: Record<string, string> = {
  rs: 'Relative Strength',
  volume: 'Volume',
  trend: 'Trend',
  sector: 'Sector',
};

export default function ModelExplanation({ detail, row }: Props) {
  const strongest = strongestFactor(detail.factors);
  const weakest = weakestFactor(detail.factors);

  const catalyst = detail.ranking?.inBreakoutScan
    ? 'Confirmed breakout in the latest scan.'
    : detail.ranking?.inPatternScan
      ? `Active pattern: ${row.pattern}.`
      : row.breakout
        ? 'Breakout flagged on row data.'
        : 'No active catalyst — momentum maintenance only.';

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
      <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
        Model Explanation
      </h4>
      <dl className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-3">
        <div className="rounded-lg border border-emerald-500/20 bg-emerald-950/20 p-3">
          <dt className="text-[10px] uppercase tracking-wider text-emerald-300">Strongest</dt>
          <dd className="mt-1 text-sm font-semibold text-emerald-100">
            {strongest ? BUCKET_LABEL[strongest.bucket] ?? strongest.bucket : 'Not available'}
          </dd>
          <dd className="mt-1 text-xs text-emerald-200/80">
            {strongest ? `Value ${strongest.value.toFixed(0)}` : 'Awaiting factor payload.'}
          </dd>
        </div>
        <div className="rounded-lg border border-blue-500/20 bg-blue-950/20 p-3">
          <dt className="text-[10px] uppercase tracking-wider text-blue-300">Catalyst</dt>
          <dd className="mt-1 text-sm font-semibold text-blue-100">
            {detail.ranking?.inBreakoutScan ? 'Breakout' : detail.ranking?.inPatternScan ? 'Pattern' : 'None'}
          </dd>
          <dd className="mt-1 text-xs text-blue-200/80">{catalyst}</dd>
        </div>
        <div className="rounded-lg border border-rose-500/20 bg-rose-950/20 p-3">
          <dt className="text-[10px] uppercase tracking-wider text-rose-300">Limiting</dt>
          <dd className="mt-1 text-sm font-semibold text-rose-100">
            {weakest ? BUCKET_LABEL[weakest.bucket] ?? weakest.bucket : 'Not available'}
          </dd>
          <dd className="mt-1 text-xs text-rose-200/80">
            {weakest ? `Value ${weakest.value.toFixed(0)}` : 'Awaiting factor payload.'}
          </dd>
        </div>
      </dl>
    </div>
  );
}
