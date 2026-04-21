import type { RunsResponse } from '@/types/api';

export const runsMock: RunsResponse = {
  stages: [
    { stage: 'Ingest', status: 'Success', duration: '2m 15s' },
    { stage: 'Features', status: 'Success', duration: '3m 12s' },
    { stage: 'Rank', status: 'Success', duration: '1m 08s' },
    { stage: 'Execute', status: 'Preview', duration: '22s' },
    { stage: 'Publish', status: 'Success', duration: '41s' },
  ],
};
