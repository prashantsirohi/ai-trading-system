import type { RunsResponse } from '@/types/api';
import { runsMock } from '@/lib/mock/runs';
import { fetchDashboardJsonStrict } from '@/lib/api/client';

export async function getRuns(): Promise<RunsResponse> {
  try {
    const res = await fetchDashboardJsonStrict<{ runs: any[] }>('/api/execution/runs', { runs: [] });
    if (res.runs?.length) {
      return {
        stages: res.runs.map((r: any) => ({
          id: r.run_id ?? 'unknown',
          status: r.status ?? 'unknown',
          stage: r.current_stage ?? 'N/A',
        })),
      } as unknown as RunsResponse;
    }
  } catch (e) {
    console.warn('Runs API failed, using mock', e);
  }
  return runsMock;
}