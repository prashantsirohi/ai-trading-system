import type { ShadowResponse } from '@/types/api';
import { shadowMock } from '@/lib/mock/shadow';
import { fetchDashboardJson } from '@/lib/api/client';

export async function getShadow(): Promise<ShadowResponse> {
  const shadowRes = await fetchDashboardJson('/api/execution/shadow', shadowMock);
  if ('detail' in shadowRes && shadowRes.detail) {
    throw new Error(`Shadow API failed: ${String(shadowRes.detail)}`);
  }
  return shadowRes as ShadowResponse;
}
