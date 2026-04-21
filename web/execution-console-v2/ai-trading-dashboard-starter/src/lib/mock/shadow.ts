import type { ShadowResponse } from '@/types/api';

export const shadowMock: ShadowResponse = {
  rows: [
    { model: 'shadow_ranker_v3', date: '2026-04-15', agreement: '78%', drift: 'Low', status: 'Healthy' },
    { model: 'pattern_classifier_v1', date: '2026-04-15', agreement: '71%', drift: 'Medium', status: 'Watch' },
    { model: 'sector_regime_probe', date: '2026-04-15', agreement: '82%', drift: 'Low', status: 'Healthy' },
  ],
};
