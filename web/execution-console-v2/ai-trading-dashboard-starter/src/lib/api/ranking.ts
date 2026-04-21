import type { RankingResponse } from '@/types/api';
import { rankingMock } from '@/lib/mock/ranking';
import { fetchDashboardJsonStrict } from '@/lib/api/client';
import { mapBackendStockRow } from '@/lib/api/mappers';

interface BackendRankingResponse {
  top_ranked?: Array<Record<string, string | number | boolean | null>>;
}

export async function getRanking(): Promise<RankingResponse> {
  const response = await fetchDashboardJsonStrict<BackendRankingResponse>(
    '/api/execution/ranking?limit=25',
    {
      top_ranked: rankingMock.rows as unknown as Array<Record<string, string | number | boolean | null>>,
    },
  );

  return {
    rows: (response.top_ranked ?? []).map(mapBackendStockRow),
  };
}
