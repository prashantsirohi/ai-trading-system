import type { PatternResponse } from '@/types/api';
import { patternsMock } from '@/lib/mock/patterns';
import { fetchDashboardJsonStrict } from '@/lib/api/client';

export async function getPatterns(): Promise<PatternResponse> {
  try {
    const marketRes = await fetchDashboardJsonStrict<{ breakouts: any[] }>('/api/execution/market', { breakouts: [] });
    if (marketRes.breakouts?.length) {
      return {
        rows: marketRes.breakouts.map((b: any) => ({
          symbol: b.symbol_id ?? 'Unknown',
          score: b.breakout_score ?? 0,
          rs: b.rel_strength_score ?? 0,
          volume: b.volume_ratio ?? 1,
          sector: b.sector ?? 'Unknown',
          breakout: b.breakout_state === 'qualified',
          pattern: b.setup_family ?? 'Breakout',
          tier: b.candidate_tier ?? 'B',
          price: b.close ?? 0,
          sectorStrength: b.sector_rs_value ?? 0,
          trend: b.symbol_trend_score ?? 0,
        })),
      } as unknown as PatternResponse;
    }
  } catch (e) {
    console.warn('Patterns API failed, using mock', e);
  }
  return patternsMock;
}