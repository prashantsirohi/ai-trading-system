import type { SectorResponse } from '@/types/api';
import type { Constituent } from '@/lib/mock/sectorConstituents';
import { sectorsMock } from '@/lib/mock/sectors';
import { fetchDashboardJsonStrict } from '@/lib/api/client';

export async function getSectors(): Promise<SectorResponse> {
  try {
    const marketRes = await fetchDashboardJsonStrict<{ sectors: any[] }>('/api/execution/market', { sectors: [] });
    if (marketRes.sectors?.length) {
      return {
        sectors: marketRes.sectors.map((s: any) => ({
          sector: s.Sector ?? s.sector ?? 'Unknown',
          rs: s.RS ?? 0,
          rs20: s.RS_20 ?? 0,
          rs50: s.RS_50 ?? 0,
          rs100: s.RS_100 ?? 0,
          momentum: s.Momentum ?? 0,
          rank: s.RS_rank ?? 0,
          rankPct: s.RS_rank_pct ?? 0,
          momentumRank: s.Momentum_rank ?? 0,
          quadrant: s.Quadrant ?? 'N/A',
        })),
      } as unknown as SectorResponse;
    }
  } catch (e) {
    console.warn('Sectors API failed, using mock', e);
  }
  return sectorsMock;
}

/**
 * Fetch sector constituents from the live ranking endpoint and derive
 * technical indicator flags from available fields.
 *
 * Falls back to an empty array (never throws) so the UI stays functional
 * when the backend is unavailable.
 */
export async function getSectorConstituents(sectorName: string): Promise<Constituent[]> {
  try {
    const res = await fetchDashboardJsonStrict<{ top_ranked?: any[] }>(
      '/api/execution/ranking?limit=500',
      { top_ranked: [] },
    );
    const rows: any[] = res.top_ranked ?? [];

    // Filter to this sector (case-insensitive)
    const sectorLower = sectorName.toLowerCase();
    const sectorRows = rows.filter(
      (r) => (r.sector_name ?? r.sector ?? '').toLowerCase() === sectorLower,
    );

    return sectorRows.map((r): Constituent => {
      const price   = Number(r.close ?? 0);
      const score   = Number(r.composite_score ?? 0);
      const sma50   = Number(r.sma_50 ?? 0);
      const sma20   = Number(r.sma_20 ?? 0);
      const sma150  = Number(r.sma_150 ?? 0);
      const high52w = Number(r.high_52w ?? 0);
      const adx     = Number(r.adx_14 ?? 0);
      const vol     = Number(r.volume ?? 0);
      const volAvg  = Number(r.vol_20_avg ?? 1);
      const ret20   = Number(r.return_20 ?? 0);   // 20-day return as decimal

      const aboveMa50   = sma50 > 0 && price > sma50;
      const aboveMa200  = sma150 > 0 && price > sma150;
      const goldenCross = sma20 > 0 && sma50 > 0 && sma20 > sma50;
      const near52wHigh = high52w > 0 && (high52w - price) / high52w <= 0.05;
      const volMult     = volAvg > 0 ? vol / volAvg : 1;
      // rel_strength_score 0–100; treat 40–70 as RSI-equivalent in-range
      const rsScore     = Number(r.rel_strength_score ?? 50);
      const rsiInRange  = rsScore >= 40 && rsScore <= 70;
      const adxAbove20  = adx > 20;
      const volExpand   = volMult > 1.5;

      return {
        symbol:       String(r.symbol_id ?? r.symbol ?? ''),
        price,
        chgPct:       ret20 * 100,   // convert decimal → percent
        rsi:          Math.round(rsScore),
        ma50Pct:      sma50 > 0 ? ((price - sma50) / sma50) * 100 : 0,
        macd:         0,             // not available in ranked signals
        volMult:      Math.round(volMult * 10) / 10,
        score,
        aboveMa50,
        aboveMa200,
        goldenCross,
        rsiInRange,
        macdBullish:  false,         // not available
        adxAbove20,
        bbSqueeze:    false,         // not available
        atrRising:    false,         // not available
        volExpand,
        obvRising:    false,         // not available
        near52wHigh,
        pivotTaken:   Boolean(r.is_stage2_structural ?? false),
      };
    });
  } catch (e) {
    console.warn('getSectorConstituents failed', e);
    return [];
  }
}