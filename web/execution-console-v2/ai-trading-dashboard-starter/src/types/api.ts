import type { MetricCard, RunStage, SectorScore, ShadowModelRow, StockRow } from './dashboard';

export interface PipelineTaskStatus {
  taskId: string;
  label: string;
  status: string;
  currentStageLabel: string;
  runId: string | null;
  startedAt: string | null;
}

export interface PipelineSummaryItem {
  label: string;
  count: number;
  highlight: string;
}

export interface PipelineWorkspaceResponse {
  runId: string;
  status: string;
  trust: string;
  date: string;
  healthStatus: string;
  trustStatus: string;
  warnings: string[];
  task: PipelineTaskStatus | null;
  summaries: {
    ranked: PipelineSummaryItem;
    breakouts: PipelineSummaryItem;
    patterns: PipelineSummaryItem;
    sectors: PipelineSummaryItem;
  };
  isEmpty: boolean;
  isDegraded: boolean;
  isFailed: boolean;
  metrics: MetricCard[];
  topStocks: StockRow[];
}

export interface RankingResponse {
  rows: StockRow[];
}

export interface PatternResponse {
  rows: StockRow[];
}

export interface SectorResponse {
  sectors: SectorScore[];
}

export interface RunsResponse {
  stages: RunStage[];
}

export interface ShadowResponse {
  rows: ShadowModelRow[];
}
