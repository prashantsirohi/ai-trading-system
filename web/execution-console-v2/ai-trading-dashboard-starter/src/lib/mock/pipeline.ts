import type { PipelineWorkspaceResponse } from '@/types/api';

export const pipelineWorkspaceMock: PipelineWorkspaceResponse = {
  runId: '1042',
  status: 'Success',
  trust: 'Healthy',
  date: '2026-04-15',
  healthStatus: 'ok',
  trustStatus: 'trusted',
  warnings: [],
  task: {
    taskId: 'task-mock',
    label: 'Mock pipeline run',
    status: 'completed',
    currentStageLabel: 'completed',
    runId: 'pipeline-2026-04-15-mock',
    startedAt: '2026-04-15 09:20:00',
  },
  summaries: {
    ranked: { label: 'Ranked Summary', count: 52, highlight: 'Top: RELIANCE' },
    breakouts: { label: 'Breakout Summary', count: 14, highlight: 'Lead: RELIANCE · high_52w_breakout' },
    patterns: { label: 'Pattern Summary', count: 9, highlight: 'Lead: INFY · cup_handle' },
    sectors: { label: 'Sector Summary', count: 11, highlight: 'Top Sector: Energy' },
  },
  isEmpty: false,
  isDegraded: false,
  isFailed: false,
  metrics: [
    { label: 'Symbols Processed', value: '875', tone: 'blue' },
    { label: 'Top Ranked', value: '52', tone: 'green' },
    { label: 'Breakout Candidates', value: '14', tone: 'yellow' },
    { label: 'Pattern Setups', value: '9', tone: 'purple' },
  ],
  topStocks: [
    { symbol: 'RELIANCE', score: 8.92, rs: 92, volume: 'High', sector: 'Energy', breakout: true, pattern: 'Cup & Handle', tier: 'A', price: 2945.5, sectorStrength: 88, trend: 84 },
    { symbol: 'INFY', score: 8.21, rs: 85, volume: 'High', sector: 'IT', breakout: true, pattern: 'Round Bottom', tier: 'A', price: 1648.2, sectorStrength: 79, trend: 82 },
    { symbol: 'HDFCBANK', score: 8.45, rs: 88, volume: 'Medium', sector: 'Banking', breakout: false, pattern: 'N/A', tier: 'B', price: 1772.9, sectorStrength: 83, trend: 78 }
  ],
};
