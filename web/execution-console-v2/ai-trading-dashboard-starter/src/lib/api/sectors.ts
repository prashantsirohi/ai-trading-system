import type { SectorResponse } from '@/types/api';
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