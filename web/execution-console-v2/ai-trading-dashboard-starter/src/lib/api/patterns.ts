import type { PatternResponse } from '@/types/api';
import { patternsMock } from '@/lib/mock/patterns';
import { fetchDashboardJsonStrict, USE_MOCK_API } from '@/lib/api/client';
import { mapBackendStockRow } from '@/lib/api/mappers';

export async function getPatterns(): Promise<PatternResponse> {
  if (USE_MOCK_API) {
    return patternsMock;
  }
  const workspaceRes = await fetchDashboardJsonStrict<{ patterns?: any[]; breakouts?: any[] }>(
    '/api/execution/workspace/pipeline?limit=50',
    { patterns: [], breakouts: [] },
  );
  if (workspaceRes.patterns?.length) {
    return {
      rows: workspaceRes.patterns.map(mapBackendStockRow),
    } as unknown as PatternResponse;
  }
  if (workspaceRes.breakouts?.length) {
    return { rows: workspaceRes.breakouts.map(mapBackendStockRow) } as unknown as PatternResponse;
  }
  return { rows: [] };
}
