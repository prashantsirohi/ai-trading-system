/**
 * Fallback payloads for the per-symbol ranking detail + history endpoints.
 *
 * Used by ``fetchDashboardJsonStrict`` when ``VITE_USE_MOCK_API=true`` so the
 * ranking view stays functional offline. Shape mirrors the backend
 * (snake_case) — the camelCase mapping happens in ``lib/api/ranking.ts``.
 */
import { rankingMock } from './ranking';

export interface BackendRankingDetailFallback {
  available: boolean;
  symbol: string;
  run_id: string | null;
  ranking: {
    rank_position: number | null;
    universe_size: number;
    composite_score: number | null;
    sector_name: string | null;
    category: string | null;
    in_breakout_scan: boolean;
    in_pattern_scan: boolean;
  } | null;
  lifecycle: Record<string, { state: string; detail: string | null }>;
  decision: { verdict: string | null; confidence: string | null; reason: string | null };
  factors: Record<string, { value: number; contributors: Array<{ column: string; value: number }> }>;
  sector_context: Record<string, string | number | boolean | null> | null;
  raw_row: Record<string, string | number | boolean | null> | null;
}

export interface BackendRankingHistoryFallback {
  available: boolean;
  symbol: string;
  history: Array<{
    run_id: string;
    run_date: string;
    rank_position: number | null;
    composite_score: number | null;
  }>;
  limit: number;
}

const VERDICT_BY_TIER: Record<'A' | 'B' | 'C', { verdict: string; confidence: string; reason: string }> = {
  A: {
    verdict: 'BUY CANDIDATE',
    confidence: 'HIGH',
    reason: 'Tier-A leadership with active breakout and pattern confirmation.',
  },
  B: {
    verdict: 'HOLD / WATCH',
    confidence: 'MEDIUM',
    reason: 'Strong relative strength, awaiting volume confirmation.',
  },
  C: {
    verdict: 'REJECT',
    confidence: 'HIGH',
    reason: 'Trend or volume insufficient for execution at this time.',
  },
};

export function getRankingDetailFallback(symbol: string): BackendRankingDetailFallback {
  const row = rankingMock.rows.find((r) => r.symbol === symbol);
  if (!row) {
    return {
      available: false,
      symbol,
      run_id: null,
      ranking: null,
      lifecycle: {
        rank: { state: 'pending', detail: null },
        breakout: { state: 'pending', detail: null },
        pattern: { state: 'pending', detail: null },
        execution: { state: 'pending', detail: null },
      },
      decision: { verdict: null, confidence: null, reason: null },
      factors: {},
      sector_context: null,
      raw_row: null,
    };
  }

  const rankPosition = rankingMock.rows.findIndex((r) => r.symbol === symbol) + 1;
  const decision = VERDICT_BY_TIER[row.tier];

  return {
    available: true,
    symbol,
    run_id: 'mock-run',
    ranking: {
      rank_position: rankPosition,
      universe_size: rankingMock.rows.length,
      composite_score: row.score,
      sector_name: row.sector,
      category: row.tier === 'A' ? 'BUY' : row.tier === 'B' ? 'WATCH' : 'BLOCK',
      in_breakout_scan: row.breakout,
      in_pattern_scan: row.pattern !== 'N/A',
    },
    lifecycle: {
      rank: { state: 'complete', detail: `Ranked #${rankPosition} of ${rankingMock.rows.length}` },
      breakout: {
        state: row.breakout ? 'complete' : 'pending',
        detail: row.breakout ? 'Breakout confirmed on the latest scan.' : 'Awaiting confirmation.',
      },
      pattern: {
        state: row.pattern !== 'N/A' ? 'complete' : 'pending',
        detail: row.pattern !== 'N/A' ? row.pattern : 'No pattern detected.',
      },
      execution: {
        state: row.tier === 'A' && row.breakout ? 'active' : 'pending',
        detail: row.tier === 'A' && row.breakout ? 'Eligible for routing.' : 'Awaiting upstream confirmation.',
      },
    },
    decision,
    factors: {
      rs: { value: row.rs, contributors: [{ column: 'rs_score', value: row.rs }] },
      volume: {
        value: row.volume === 'High' ? 85 : row.volume === 'Medium' ? 60 : 35,
        contributors: [
          {
            column: 'volume_score',
            value: row.volume === 'High' ? 85 : row.volume === 'Medium' ? 60 : 35,
          },
        ],
      },
      trend: { value: row.trend, contributors: [{ column: 'trend_score', value: row.trend }] },
      sector: {
        value: row.sectorStrength,
        contributors: [{ column: 'sector_score', value: row.sectorStrength }],
      },
    },
    sector_context: { sector: row.sector, sector_strength: row.sectorStrength },
    raw_row: {
      symbol_id: row.symbol,
      composite_score: row.score,
      sector_name: row.sector,
    },
  };
}

const HISTORY_DAYS = 14;

export function getRankingHistoryFallback(
  symbol: string,
  limit: number,
): BackendRankingHistoryFallback {
  const row = rankingMock.rows.find((r) => r.symbol === symbol);
  const baseRank = row
    ? Math.max(1, rankingMock.rows.findIndex((r) => r.symbol === symbol) + 1)
    : null;

  const history = Array.from({ length: Math.min(limit, HISTORY_DAYS) }, (_, idx) => {
    const date = new Date();
    date.setDate(date.getDate() - idx);
    const isoDate = date.toISOString().slice(0, 10);
    const drift = Math.sin(idx * 0.7) * 3;
    const rankPosition = baseRank !== null ? Math.max(1, Math.round(baseRank + drift)) : null;
    const compositeScore = row ? Math.max(0, row.score - idx * 0.05 + drift * 0.05) : null;
    return {
      run_id: `mock-run-${isoDate}`,
      run_date: isoDate,
      rank_position: rankPosition,
      composite_score: compositeScore !== null ? Number(compositeScore.toFixed(2)) : null,
    };
  });

  return {
    available: true,
    symbol,
    history,
    limit,
  };
}
