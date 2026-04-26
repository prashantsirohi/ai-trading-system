/**
 * Derives the five-cell data-quality strip (Quantis proposal #07) from the
 * existing pipeline workspace payload.
 *
 * No dedicated DQ endpoint exists yet, so each cell is computed from the
 * signals we already have (warnings, counts, trust state). Documented here
 * so the swap-out is obvious when a real ``/pipeline/dq`` lands.
 */
import type { PipelineWorkspaceResponse } from '@/types/api';

export type DqTone = 'pass' | 'warn' | 'fail';

export interface DqCell {
  label: string;
  value: string;
  hint: string;
  tone: DqTone;
}

function classifyWarning(message: string): {
  bucket: 'stale' | 'missing' | 'schema' | 'lag' | 'coverage' | 'other';
} {
  const m = message.toLowerCase();
  if (m.includes('stale') || m.includes('not ticked')) return { bucket: 'stale' };
  if (m.includes('missing') || m.includes('absent') || m.includes('gap'))
    return { bucket: 'missing' };
  if (m.includes('schema') || m.includes('column') || m.includes('type mismatch'))
    return { bucket: 'schema' };
  if (m.includes('lag') || m.includes('latency') || m.includes('delay'))
    return { bucket: 'lag' };
  if (m.includes('coverage') || m.includes('universe')) return { bucket: 'coverage' };
  return { bucket: 'other' };
}

export function deriveDqCells(payload: PipelineWorkspaceResponse | undefined): DqCell[] {
  const warnings = payload?.warnings ?? [];
  const buckets = {
    stale: 0,
    missing: 0,
    schema: 0,
    lag: 0,
    coverage: 0,
    other: 0,
  };
  for (const w of warnings) {
    buckets[classifyWarning(w).bucket] += 1;
  }

  const trustStatus = payload?.trustStatus ?? 'unknown';
  const healthStatus = payload?.healthStatus ?? 'unknown';
  const isStale = trustStatus === 'degraded' || trustStatus === 'blocked';

  const stalePrices: DqCell = {
    label: 'Stale prices',
    value: buckets.stale > 0 ? String(buckets.stale) : isStale ? '?' : '0',
    hint: 'Symbols not ticked in last 5m',
    tone: buckets.stale > 0 ? 'fail' : isStale ? 'warn' : 'pass',
  };

  const missingRows: DqCell = {
    label: 'Missing rows',
    value: buckets.missing > 0 ? String(buckets.missing) : '0',
    hint: 'Bars expected vs received (1h)',
    tone: buckets.missing > 0 ? (buckets.missing > 3 ? 'fail' : 'warn') : 'pass',
  };

  const schemaBreaks: DqCell = {
    label: 'Schema',
    value: buckets.schema > 0 ? String(buckets.schema) : 'OK',
    hint: 'Validation errors today',
    tone: buckets.schema > 0 ? 'fail' : 'pass',
  };

  const vendorLag: DqCell = {
    label: 'Vendor lag',
    value:
      buckets.lag > 0
        ? String(buckets.lag)
        : healthStatus === 'warn'
          ? 'high'
          : 'normal',
    hint: 'vs benchmark feed',
    tone: buckets.lag > 0 ? 'warn' : healthStatus === 'warn' ? 'warn' : 'pass',
  };

  const coverage: DqCell = {
    label: 'Coverage',
    value:
      buckets.coverage > 0
        ? `−${buckets.coverage}`
        : payload?.isEmpty
          ? '0%'
          : '100%',
    hint: 'Universe currently flowing',
    tone: payload?.isEmpty ? 'fail' : buckets.coverage > 0 ? 'warn' : 'pass',
  };

  return [stalePrices, missingRows, schemaBreaks, vendorLag, coverage];
}
