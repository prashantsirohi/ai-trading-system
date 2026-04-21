import type { ShadowResponse } from '@/types/api';
import { shadowMock } from '@/lib/mock/shadow';
import { fetchDashboardJson } from '@/lib/api/client';

export async function getShadow(): Promise<ShadowResponse> {
  const shadowRes = await fetchDashboardJson('/api/execution/shadow', shadowMock);
  // Handle case where backend returns error (shadow has 500 bug)
  if ('detail' in shadowRes && shadowRes.detail) {
    console.warn('Shadow API returned error, using mock data');
    return shadowMock;
  }
  return shadowRes as ShadowResponse;
}