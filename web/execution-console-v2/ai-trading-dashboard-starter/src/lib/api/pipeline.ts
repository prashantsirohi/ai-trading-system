import type { PipelineWorkspaceResponse } from '@/types/api';
import { pipelineWorkspaceMock } from '@/lib/mock/pipeline';
import { fetchDashboardJsonStrict } from '@/lib/api/client';
import type { MetricCard } from '@/types/dashboard';
import { mapBackendStockRow } from '@/lib/api/mappers';

interface BackendPipelineWorkspaceResponse {
  warnings?: string[];
  summary?: {
    top_sector?: string;
  };
  health?: {
    status?: string;
    summary?: {
      latest_delivery_date?: string;
      latest_ohlcv_date?: string;
    };
  };
  data_trust?: {
    status?: string;
    latest_trade_date?: string;
  };
  counts?: {
    ranked?: number;
    breakouts?: number;
    patterns?: number;
    sectors?: number;
    stock_scan?: number;
  };
  breakouts?: Array<Record<string, string | number | boolean | null>>;
  patterns?: Array<Record<string, string | number | boolean | null>>;
  sectors?: Array<Record<string, string | number | boolean | null>>;
  top_ranked?: Array<Record<string, string | number | boolean | null>>;
  artifact_path?: string | null;
}

interface BackendTask {
  task_id?: string;
  label?: string;
  status?: string;
  current_stage_label?: string;
  run_id?: string | null;
  started_at?: string | null;
  operator_action_type?: string;
  task_type?: string;
}

interface BackendTasksResponse {
  tasks?: BackendTask[];
}

function buildMetrics(payload: BackendPipelineWorkspaceResponse): MetricCard[] {
  const counts = payload.counts ?? {};
  return [
    { label: 'Ranked Signals', value: String(counts.ranked ?? 0), tone: 'blue' },
    { label: 'Breakouts', value: String(counts.breakouts ?? 0), tone: 'yellow' },
    { label: 'Patterns', value: String(counts.patterns ?? 0), tone: 'purple' },
    { label: 'Sectors', value: String(counts.sectors ?? 0), tone: 'green' },
  ];
}

function extractRunId(payload: BackendPipelineWorkspaceResponse): string {
  const artifactPath = payload.artifact_path ?? '';
  const match = /pipeline-[^/]+/.exec(artifactPath);
  return match?.[0] ?? 'latest';
}

function normalizeHealthStatus(value: string | undefined): string {
  return String(value ?? 'unknown').toLowerCase();
}

function normalizeTrustStatus(value: string | undefined): string {
  return String(value ?? 'unknown').toLowerCase();
}

function pickPipelineTask(tasks: BackendTask[]): BackendTask | null {
  const pipelineTask = tasks.find((task) => {
    const actionType = String(task.operator_action_type ?? '').toLowerCase();
    const taskType = String(task.task_type ?? '').toLowerCase();
    return actionType === 'pipeline_task' || taskType === 'pipeline';
  });
  if (pipelineTask) {
    return pipelineTask;
  }

  const stageScopedTask = tasks.find((task) => {
    const label = String(task.label ?? '').toLowerCase();
    const actionType = String(task.operator_action_type ?? '').toLowerCase();
    return (
      actionType.includes('publish') ||
      actionType.includes('execute') ||
      label.includes('daily pipeline') ||
      label.includes('publish retry') ||
      label.includes('shadow refresh')
    );
  });

  return stageScopedTask ?? null;
}

function summaryHighlight(value: string | number | boolean | null | undefined, fallback: string): string {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }
  return String(value);
}

export async function getPipelineWorkspace(): Promise<PipelineWorkspaceResponse> {
  const workspaceResponse = await fetchDashboardJsonStrict<BackendPipelineWorkspaceResponse>(
    '/api/execution/workspace/pipeline?limit=20',
    {
      warnings: pipelineWorkspaceMock.warnings,
      summary: { top_sector: 'N/A' },
      top_ranked: pipelineWorkspaceMock.topStocks as unknown as Array<Record<string, string | number | boolean | null>>,
      breakouts: [],
      patterns: [],
      sectors: [],
      counts: {
        ranked: pipelineWorkspaceMock.topStocks.length,
        breakouts: 0,
        patterns: 0,
        sectors: 0,
      },
      health: {
        status: pipelineWorkspaceMock.status,
        summary: {
          latest_delivery_date: pipelineWorkspaceMock.date,
        },
      },
      data_trust: {
        status: pipelineWorkspaceMock.trust,
        latest_trade_date: pipelineWorkspaceMock.date,
      },
      artifact_path: pipelineWorkspaceMock.runId,
    },
  );

  const tasksResponse = await fetchDashboardJsonStrict<BackendTasksResponse>(
    '/api/execution/tasks?limit=20',
    { tasks: [] },
  );

  const counts = workspaceResponse.counts ?? {};
  const status = workspaceResponse.health?.status ?? pipelineWorkspaceMock.status;
  const trust = workspaceResponse.data_trust?.status ?? pipelineWorkspaceMock.trust;
  const date =
    workspaceResponse.health?.summary?.latest_delivery_date ??
    workspaceResponse.health?.summary?.latest_ohlcv_date ??
    workspaceResponse.data_trust?.latest_trade_date ??
    pipelineWorkspaceMock.date;
  const healthStatus = normalizeHealthStatus(workspaceResponse.health?.status);
  const trustStatus = normalizeTrustStatus(workspaceResponse.data_trust?.status);
  const warnings = (workspaceResponse.warnings ?? []).map(String);

  const selectedTask = pickPipelineTask(tasksResponse.tasks ?? []);
  const rankedCount = Number(counts.ranked ?? 0);
  const breakoutCount = Number(counts.breakouts ?? 0);
  const patternCount = Number(counts.patterns ?? 0);
  const sectorCount = Number(counts.sectors ?? 0);
  const noData =
    rankedCount + breakoutCount + patternCount + sectorCount === 0 &&
    (workspaceResponse.top_ranked ?? []).length === 0 &&
    (workspaceResponse.breakouts ?? []).length === 0 &&
    (workspaceResponse.patterns ?? []).length === 0 &&
    (workspaceResponse.sectors ?? []).length === 0;

  const taskStatus = String(selectedTask?.status ?? '').toLowerCase();
  const taskFailed = taskStatus === 'failed' || taskStatus === 'terminated';
  const failed = healthStatus === 'error' || healthStatus === 'failed' || taskFailed;
  const degraded =
    !failed &&
    (
      healthStatus === 'warn' ||
      trustStatus === 'degraded' ||
      trustStatus === 'blocked' ||
      trustStatus === 'legacy' ||
      warnings.length > 0
    );

  return {
    runId: extractRunId(workspaceResponse),
    status,
    trust,
    date,
    healthStatus,
    trustStatus,
    warnings,
    task: selectedTask
      ? {
          taskId: String(selectedTask.task_id ?? 'unknown-task'),
          label: String(selectedTask.label ?? 'Pipeline task'),
          status: String(selectedTask.status ?? 'unknown'),
          currentStageLabel: String(selectedTask.current_stage_label ?? 'N/A'),
          runId: selectedTask.run_id ? String(selectedTask.run_id) : null,
          startedAt: selectedTask.started_at ? String(selectedTask.started_at) : null,
        }
      : null,
    summaries: {
      ranked: {
        label: 'Ranked Summary',
        count: rankedCount,
        highlight: `Top: ${summaryHighlight(workspaceResponse.top_ranked?.[0]?.symbol_id, 'N/A')}`,
      },
      breakouts: {
        label: 'Breakout Summary',
        count: breakoutCount,
        highlight: `Lead: ${summaryHighlight(workspaceResponse.breakouts?.[0]?.symbol_id, 'N/A')} · ${summaryHighlight(workspaceResponse.breakouts?.[0]?.setup_family, 'N/A')}`,
      },
      patterns: {
        label: 'Pattern Summary',
        count: patternCount,
        highlight: `Lead: ${summaryHighlight(workspaceResponse.patterns?.[0]?.symbol_id, 'N/A')} · ${summaryHighlight(workspaceResponse.patterns?.[0]?.pattern_family, 'N/A')}`,
      },
      sectors: {
        label: 'Sector Summary',
        count: sectorCount,
        highlight: `Top Sector: ${summaryHighlight(workspaceResponse.summary?.top_sector ?? workspaceResponse.sectors?.[0]?.Sector ?? workspaceResponse.sectors?.[0]?.sector, 'N/A')}`,
      },
    },
    isEmpty: noData,
    isDegraded: degraded,
    isFailed: failed,
    metrics: buildMetrics(workspaceResponse),
    topStocks: (workspaceResponse.top_ranked ?? []).map(mapBackendStockRow),
  };
}
