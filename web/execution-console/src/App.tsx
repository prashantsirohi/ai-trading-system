import { useEffect, useMemo, useState } from "react";
import { NavLink, Route, Routes, useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { eventStreamUrl, fetchJson, postJson, type AnyRecord } from "./api";
import { EXECUTION_ACTIONS, getExecutionAction, type ExecutionActionDefinition, type ExecutionActionId } from "./actions";
import { useExecutionUiStore } from "./store";

type SummaryResponse = {
  db_stats: AnyRecord;
  health: { status: string; summary?: AnyRecord; checks?: AnyRecord[] };
  latest_run?: AnyRecord | null;
  active_task_count: number;
  task_count: number;
  payload: { artifact_path?: string; summary?: AnyRecord };
};

type TaskResponse = {
  task: AnyRecord;
};

type TaskListResponse = {
  tasks: AnyRecord[];
};

type TaskSnapshotResponse = {
  task: AnyRecord;
  logs: Array<{ message: string; log_cursor: number }>;
};

const navItems = [
  { to: "/pipeline", label: "Pipeline" },
  { to: "/control", label: "Control" },
  { to: "/ranking", label: "Ranking" },
  { to: "/market", label: "Market" },
  { to: "/operations", label: "Operations" },
  { to: "/shadow", label: "Shadow" },
  { to: "/tasks", label: "Tasks" },
  { to: "/processes", label: "Processes" },
];

type PipelineWorkspaceResponse = {
  artifact_path?: string;
  summary?: AnyRecord;
  warnings?: unknown[];
  health?: { status?: string; summary?: AnyRecord; checks?: AnyRecord[] };
  ops_health?: {
    available?: boolean;
    stages?: Record<string, AnyRecord>;
    stale_stages?: string[];
    dq_summary?: AnyRecord;
  };
  data_trust?: AnyRecord;
  top_ranked: AnyRecord[];
  breakouts: AnyRecord[];
  patterns: AnyRecord[];
  sectors: AnyRecord[];
  stock_scan: AnyRecord[];
  counts?: AnyRecord;
};

function App() {
  return (
    <div className="shell">
      <Sidebar />
      <main className="workspace">
        <TopBar />
        <ToastHost />
        <Routes>
          <Route path="/" element={<PipelinePage />} />
          <Route path="/pipeline" element={<PipelinePage />} />
          <Route path="/control" element={<ControlPage />} />
          <Route path="/ranking" element={<RankingPage />} />
          <Route path="/market" element={<MarketPage />} />
          <Route path="/operations" element={<OperationsPage />} />
          <Route path="/shadow" element={<ShadowPage />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/processes" element={<ProcessesPage />} />
        </Routes>
      </main>
      <TaskDrawer />
    </div>
  );
}

function PipelinePage() {
  const pipelineQuery = useQuery({
    queryKey: ["pipeline-workspace"],
    queryFn: () => fetchJson<PipelineWorkspaceResponse>("/api/execution/workspace/pipeline?limit=20"),
    refetchInterval: 10000,
  });

  const payload = pipelineQuery.data;
  const health = payload?.health;
  const summary = health?.summary ?? {};
  const trust = payload?.data_trust ?? {};
  const trustStatus = String(trust?.status ?? "—");
  const warnings = Array.isArray(payload?.warnings) ? payload?.warnings : [];

  return (
    <div className="page-grid">
      <Panel title="Pipeline Workspace" subtitle="First React operator page over the latest operational rank artifact" className="span-3">
        <OpsHealthRibbon snapshot={payload?.ops_health} health={health} trust={trust} />
      </Panel>
      <MetricCard title="Health" value={String(health?.status?.toUpperCase() ?? "LOADING")} detail={`OHLCV ${String(summary?.latest_ohlcv_date ?? "—")}`} tone={String(health?.status ?? "warn")} />
      <MetricCard title="Trust" value={trustStatus.toUpperCase()} detail={`Validated ${String(trust?.latest_validated_date ?? "—")}`} tone={trustStatus.toLowerCase()} />
      <MetricCard title="Breakouts" value={Number(payload?.counts?.breakouts ?? 0)} detail="Latest breakout scan rows" tone="success" />
      <MetricCard title="Patterns" value={Number(payload?.counts?.patterns ?? 0)} detail="Latest pattern scan rows" tone="info" />
      <Panel title="Top Ranked Signals" subtitle="Current leaderboard from ranked_signals.csv" className="span-2">
        <DataTable rows={payload?.top_ranked ?? []} />
      </Panel>
      <Panel title="Breakout Monitor" subtitle="Current breakout scan rows" className="span-2">
        <DataTable rows={payload?.breakouts ?? []} />
      </Panel>
      <Panel title="Pattern Monitor" subtitle="Operational pattern signals" className="span-2">
        <DataTable rows={payload?.patterns ?? []} />
      </Panel>
      <Panel title="Sector Dashboard" subtitle="Sector leadership snapshot">
        <DataTable rows={payload?.sectors ?? []} />
      </Panel>
      <Panel title="Stock Scan" subtitle="Supplementary stock scan rows">
        <DataTable rows={payload?.stock_scan ?? []} />
      </Panel>
      <Panel title="Artifact & Warnings" subtitle="Pipeline artifact provenance and any surfaced warnings" className="span-3">
        <KeyValueGrid
          items={[
            ["Artifact", payload?.artifact_path],
            ["Payload Age (min)", summary?.payload_age_minutes],
            ["Delivery Date", summary?.latest_delivery_date],
            ["Validated Date", trust?.latest_validated_date],
          ]}
        />
        {warnings.length ? <DataTable rows={warnings.map((warning, index) => ({ id: index + 1, warning }))} /> : <div className="empty-state">No warnings surfaced in the latest payload.</div>}
      </Panel>
    </div>
  );
}

function useLiveTask(taskId: string | null) {
  const [taskState, setTaskState] = useState<AnyRecord | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [cursor, setCursor] = useState(0);

  useEffect(() => {
    if (!taskId) {
      setTaskState(null);
      setLogs([]);
      setCursor(0);
      return;
    }
    setLogs([]);
    setCursor(0);
    setTaskState(null);
    const source = new EventSource(eventStreamUrl(`/api/execution/tasks/${taskId}/events?cursor=0`));
    source.onmessage = (event) => {
      const payload = JSON.parse(event.data) as { task?: AnyRecord; logs?: Array<{ message: string; log_cursor: number }>; cursor?: number };
      if (payload.task) {
        setTaskState(payload.task);
      }
      if (payload.logs?.length) {
        const newLogs = payload.logs.map((row) => row.message);
        setLogs((current) => [...current, ...newLogs]);
      }
      if (typeof payload.cursor === "number") {
        setCursor(payload.cursor);
      }
    };
    source.onerror = () => {
      source.close();
    };
    return () => source.close();
  }, [taskId]);

  return { taskState, logs, cursor };
}

function Sidebar() {
  const summaryQuery = useQuery({
    queryKey: ["execution-summary"],
    queryFn: () => fetchJson<SummaryResponse>("/api/execution/summary"),
    refetchInterval: 10000,
  });
  const status = summaryQuery.data?.health?.status?.toUpperCase() ?? "LOADING";

  return (
    <aside className="sidebar">
      <div className="brand-card">
        <div className="eyebrow">Operator Surface</div>
        <h1>Execution Console</h1>
        <p>React operator shell over the pipeline control plane, ranked artifacts, and task stream.</p>
        <div className={`status-pill status-${(summaryQuery.data?.health?.status ?? "warn").toLowerCase()}`}>{status}</div>
      </div>
      <nav className="nav">
        {navItems.map((item) => (
          <NavLink key={item.to} to={item.to} className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
            {item.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}

function TopBar() {
  const queryClient = useQueryClient();
  const [pendingActionId, setPendingActionId] = useState<ExecutionActionId | null>(null);
  const [launchingActionId, setLaunchingActionId] = useState<ExecutionActionId | null>(null);
  const [progressModalTaskId, setProgressModalTaskId] = useState<string | null>(null);
  const setActiveTask = useExecutionUiStore((state) => state.setActiveTask);
  const setSelectedRun = useExecutionUiStore((state) => state.setSelectedRun);
  const selectTask = useExecutionUiStore((state) => state.selectTask);
  const showToast = useExecutionUiStore((state) => state.showToast);

  const launch = useMutation({
    mutationFn: async (action: ExecutionActionDefinition) => {
      setLaunchingActionId(action.id);
      return postJson<TaskResponse>(action.apiPath, action.requestBody);
    },
    onSuccess: (result, action) => {
      const taskId = String(result.task.task_id ?? "");
      if (taskId) {
        setActiveTask(taskId);
        setProgressModalTaskId(taskId);
      }
      const runId = result.task.run_id ? String(result.task.run_id) : null;
      if (runId) {
        setSelectedRun(runId);
      }
      showToast(action.label, `${action.summary} Task ${taskId || "created"} is now running in the background.`);
      setPendingActionId(null);
      setLaunchingActionId(null);
      void queryClient.invalidateQueries();
    },
    onError: (error, action) => {
      showToast(action.label, `Launch failed: ${error instanceof Error ? error.message : "Unknown error"}`);
      setLaunchingActionId(null);
    },
  });

  return (
    <>
      <header className="topbar">
        <div>
          <div className="eyebrow">Execution API</div>
          <h2>Broker-style control surface</h2>
        </div>
        <div className="action-grid">
          {EXECUTION_ACTIONS.map((action) => (
            <button
              key={action.id}
              className={`action-button action-${action.sideEffectLevel}`}
              onClick={() => setPendingActionId(action.id)}
              disabled={launch.isPending}
            >
              <span className="action-button-title">{action.label}</span>
              <span className="action-button-subtitle">{action.shortLabel}</span>
              <span className="action-badge-row">
                {action.badges.map((badge) => (
                  <span key={badge} className="action-badge">
                    {badge}
                  </span>
                ))}
              </span>
            </button>
          ))}
        </div>
      </header>
      {pendingActionId ? (
        <ActionConfirmModal
          action={getExecutionAction(pendingActionId)}
          isLaunching={launch.isPending && launchingActionId === pendingActionId}
          onCancel={() => setPendingActionId(null)}
          onConfirm={() => launch.mutate(getExecutionAction(pendingActionId))}
        />
      ) : null}
      {progressModalTaskId ? (
        <TaskProgressModal
          taskId={progressModalTaskId}
          onClose={() => setProgressModalTaskId(null)}
          onOpenLogs={(taskId) => selectTask(taskId)}
        />
      ) : null}
    </>
  );
}

function ToastHost() {
  const toast = useExecutionUiStore((state) => state.toast);
  const clearToast = useExecutionUiStore((state) => state.clearToast);

  useEffect(() => {
    if (!toast) return;
    const timer = window.setTimeout(() => clearToast(), 4500);
    return () => window.clearTimeout(timer);
  }, [toast, clearToast]);

  if (!toast) return null;

  return (
    <div className="toast-host">
      <div className="toast-card">
        <strong>{toast.title}</strong>
        <p>{toast.body}</p>
      </div>
    </div>
  );
}

function ActionConfirmModal(props: {
  action: ExecutionActionDefinition;
  isLaunching: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div className="modal-backdrop">
      <div className="modal-card">
        <div className="panel-header">
          <div>
            <div className="eyebrow">Before You Run</div>
            <h3>{props.action.label}</h3>
            <p>{props.action.purpose}</p>
          </div>
        </div>
        <div className="brief-grid">
          <div className="brief-block">
            <span>Summary</span>
            <strong>{props.action.summary}</strong>
          </div>
          <div className="brief-block">
            <span>Estimated Duration</span>
            <strong>{props.action.estimatedDuration}</strong>
          </div>
          <div className="brief-block">
            <span>Publish Included</span>
            <strong>{props.action.publishIncluded ? "Yes" : "No"}</strong>
          </div>
          <div className="brief-block">
            <span>Risk / Trust Note</span>
            <strong>{props.action.riskNote}</strong>
          </div>
        </div>
        <div className="modal-section">
          <div className="eyebrow">Stages / Modules</div>
          <ul className="simple-list">
            {props.action.stageLabels.map((stage) => (
              <li key={stage}>{stage}</li>
            ))}
          </ul>
        </div>
        <div className="modal-section">
          <div className="eyebrow">Expected Outputs</div>
          <ul className="simple-list">
            {props.action.expectedOutputs.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
        <div className="modal-footer">
          <button className="ghost-button" onClick={props.onCancel} disabled={props.isLaunching}>
            Cancel
          </button>
          <button className="primary-button" onClick={props.onConfirm} disabled={props.isLaunching}>
            {props.isLaunching ? "Starting…" : "Run Now"}
          </button>
        </div>
      </div>
    </div>
  );
}

function TaskProgressModal(props: {
  taskId: string;
  onClose: () => void;
  onOpenLogs: (taskId: string) => void;
}) {
  const navigate = useNavigate();
  const setSelectedRun = useExecutionUiStore((state) => state.setSelectedRun);
  const { taskState, logs } = useLiveTask(props.taskId);
  const status = String(taskState?.status ?? "running").toLowerCase();
  const runId = taskState?.run_id ? String(taskState.run_id) : null;
  const tailLogs = logs.slice(-6);

  return (
    <div className="modal-backdrop">
      <div className="modal-card progress-modal">
        <div className="progress-modal-header">
          <div>
            <div className="eyebrow">Live Execution Progress</div>
            <h3>{String(taskState?.label ?? props.taskId)}</h3>
            <p>{String(taskState?.current_stage_label ?? taskState?.phase_label ?? "Starting background task")}</p>
          </div>
          <div className="progress-actions">
            <span className={`status-pill status-${status}`}>{status.toUpperCase()}</span>
            <button className="ghost-button" onClick={props.onClose}>
              Minimize
            </button>
          </div>
        </div>

        <KeyValueGrid
          items={[
            ["Task ID", props.taskId],
            ["Action", taskState?.origin_action],
            ["Started", taskState?.started_at],
            ["Run ID", runId],
            ["Current Step", taskState?.current_stage_label ?? taskState?.phase_label],
            ["Finished", taskState?.finished_at],
          ]}
        />

        <div className="progress-modal-body">
          <StageTimeline rows={(taskState?.stage_statuses as AnyRecord[]) ?? []} />
          <PublishProgressPanel publishProgress={taskState?.publish_progress as AnyRecord | undefined} />
        </div>

        {status === "failed" ? <div className="error-banner">{String(taskState?.error ?? "Task failed without an error message.")}</div> : null}

        <div className="progress-log-preview">
          <div className="panel-header">
            <div>
              <div className="eyebrow">Recent Activity</div>
              <p>Latest background updates from this task.</p>
            </div>
          </div>
          <div className="progress-log-lines">
            {tailLogs.length === 0 ? <div className="empty-state">Waiting for live task updates…</div> : tailLogs.map((line, index) => <pre key={`${index}-${line}`}>{line}</pre>)}
          </div>
        </div>

        <div className="modal-footer">
          <button className="ghost-button" onClick={props.onClose}>
            Close Popup
          </button>
          <button className="secondary-button" onClick={() => props.onOpenLogs(props.taskId)}>
            Open Live Logs
          </button>
          <button
            className="primary-button"
            onClick={() => {
              if (runId) {
                setSelectedRun(runId);
                navigate("/operations");
              }
            }}
            disabled={!runId}
          >
            Open Run Details
          </button>
        </div>
      </div>
    </div>
  );
}

function ControlPage() {
  const summaryQuery = useQuery({
    queryKey: ["execution-summary"],
    queryFn: () => fetchJson<SummaryResponse>("/api/execution/summary"),
    refetchInterval: 10000,
  });
  const tasksQuery = useQuery({
    queryKey: ["tasks"],
    queryFn: () => fetchJson<TaskListResponse>("/api/execution/tasks?limit=20"),
    refetchInterval: 5000,
  });

  const summary = summaryQuery.data;
  const recentActions = (tasksQuery.data?.tasks ?? []).filter((task) => task.origin_action).slice(0, 5);

  return (
    <div className="page-grid">
      <ProgressCenter />
      <MetricCard title="Health" value={summary?.health?.status?.toUpperCase() ?? "LOADING"} detail="Control-plane trust and freshness status" tone={summary?.health?.status ?? "warn"} />
      <MetricCard title="Active Tasks" value={summary?.active_task_count ?? "—"} detail="Operator jobs currently running" tone="info" />
      <MetricCard title="Symbols" value={Number(summary?.db_stats?.symbols ?? 0)} detail="Tracked NSE symbols in operational store" tone="success" />
      <MetricCard title="Latest Run" value={String(summary?.latest_run?.run_id ?? "—")} detail={String(summary?.latest_run?.status ?? "No run yet")} tone="neutral" />
      <Panel title="Pipeline Snapshot" subtitle="Latest run and payload context">
        <KeyValueGrid
          items={[
            ["Latest Run", summary?.latest_run?.run_id],
            ["Run Status", summary?.latest_run?.status],
            ["Current Stage", summary?.latest_run?.current_stage],
            ["Artifact", summary?.payload?.artifact_path],
            ["Latest OHLC", summary?.health?.summary?.latest_ohlcv_date],
            ["Latest Delivery", summary?.health?.summary?.latest_delivery_date],
          ]}
        />
      </Panel>
      <Panel title="Trust Checks" subtitle="Current health checks from the execution service">
        <DataTable rows={summary?.health?.checks ?? []} />
      </Panel>
      <Panel title="Recent Operator Actions" subtitle="Last five launched top-bar actions" className="span-3">
        <DataTable rows={recentActions} />
      </Panel>
    </div>
  );
}

function ProgressCenter() {
  const navigate = useNavigate();
  const activeTaskId = useExecutionUiStore((state) => state.activeTaskId);
  const dismissedForTaskId = useExecutionUiStore((state) => state.progressDismissedForTaskId);
  const dismissProgress = useExecutionUiStore((state) => state.dismissProgress);
  const selectTask = useExecutionUiStore((state) => state.selectTask);
  const setSelectedRun = useExecutionUiStore((state) => state.setSelectedRun);
  const { taskState } = useLiveTask(activeTaskId);

  if (!activeTaskId || dismissedForTaskId === activeTaskId || !taskState) {
    return null;
  }

  const status = String(taskState.status ?? "unknown").toLowerCase();
  const compact = status === "completed";
  const stageStatuses = Array.isArray(taskState.stage_statuses) ? (taskState.stage_statuses as AnyRecord[]) : [];
  const runId = taskState.run_id ? String(taskState.run_id) : null;

  return (
    <section className={`panel progress-center span-3 ${compact ? "progress-compact" : ""}`}>
      <div className="progress-header">
        <div>
          <div className="eyebrow">Execution Progress Center</div>
          <h3>{String(taskState.label ?? activeTaskId)}</h3>
          <p>{String(taskState.current_stage_label ?? taskState.phase_label ?? "Running background task")}</p>
        </div>
        <div className="progress-actions">
          <span className={`status-pill status-${status}`}>{status.toUpperCase()}</span>
          <button className="ghost-button" onClick={() => dismissProgress(activeTaskId)}>
            Dismiss
          </button>
        </div>
      </div>
      <KeyValueGrid
        items={[
          ["Task ID", activeTaskId],
          ["Started", taskState.started_at],
          ["Finished", taskState.finished_at],
          ["Run ID", runId],
          ["Action", taskState.origin_action],
          ["Current Step", taskState.current_stage_label ?? taskState.phase_label],
        ]}
      />
      {!compact ? <StageTimeline rows={stageStatuses} /> : null}
      {status === "failed" ? <div className="error-banner">{String(taskState.error ?? "Task failed without an error message.")}</div> : null}
      <div className="progress-link-row">
        <button className="secondary-button" onClick={() => selectTask(activeTaskId)}>
          Open Live Logs
        </button>
        <button
          className="secondary-button"
          onClick={() => {
            if (runId) {
              setSelectedRun(runId);
              navigate("/operations");
            }
          }}
          disabled={!runId}
        >
          Open Run Details
        </button>
      </div>
    </section>
  );
}

function StageTimeline(props: { rows: AnyRecord[] }) {
  if (!props.rows.length) {
    return <div className="empty-state">Stage timeline will appear when pipeline stage metadata is available.</div>;
  }
  return (
    <div className="timeline">
      {props.rows.map((row) => (
        <div key={String(row.stage_name)} className={`timeline-step timeline-${String(row.status ?? "pending").toLowerCase()}`}>
          <div className="timeline-dot" />
          <div>
            <strong>{String(row.label ?? row.stage_name ?? "Stage")}</strong>
            <p>{String(row.status ?? "pending").toUpperCase()}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

function RankingPage() {
  const rankingQuery = useQuery({
    queryKey: ["ranking"],
    queryFn: () => fetchJson<{ top_ranked: AnyRecord[]; chart: AnyRecord[]; artifact_count: number }>("/api/execution/ranking?limit=25"),
    refetchInterval: 10000,
  });

  return (
    <div className="page-grid">
      <MetricCard title="Ranked Rows" value={rankingQuery.data?.artifact_count ?? "—"} detail="Latest rank artifact coverage" tone="success" />
      <Panel title="Composite Leaders" subtitle="Top symbols from the latest operational rank artifact" className="span-2">
        <ChartCard rows={rankingQuery.data?.chart ?? []} xKey="symbol_id" yKey="composite_score" />
      </Panel>
      <Panel title="Top Ranked Signals" subtitle="Operator view of the current leaderboard" className="span-3">
        <DataTable rows={rankingQuery.data?.top_ranked ?? []} />
      </Panel>
    </div>
  );
}

function MarketPage() {
  const marketQuery = useQuery({
    queryKey: ["market"],
    queryFn: () => fetchJson<{ breakouts: AnyRecord[]; sectors: AnyRecord[]; health: AnyRecord }>("/api/execution/market?limit=25"),
    refetchInterval: 10000,
  });
  return (
    <div className="page-grid">
      <Panel title="Breakout Monitor" subtitle="Latest breakout candidates from rank-stage artifact" className="span-2">
        <DataTable rows={marketQuery.data?.breakouts ?? []} />
      </Panel>
      <Panel title="Sector Leadership" subtitle="Current sector dashboard rows" className="span-2">
        <DataTable rows={marketQuery.data?.sectors ?? []} />
      </Panel>
      <Panel title="Health Checks" subtitle="Market trust and schema checks" className="span-2">
        <DataTable rows={(marketQuery.data?.health?.checks as AnyRecord[]) ?? []} />
      </Panel>
    </div>
  );
}

function OperationsPage() {
  const selectedRunId = useExecutionUiStore((state) => state.selectedRunId);
  const setSelectedRun = useExecutionUiStore((state) => state.setSelectedRun);
  const runsQuery = useQuery({
    queryKey: ["runs"],
    queryFn: () => fetchJson<{ runs: AnyRecord[] }>("/api/execution/runs?limit=20"),
    refetchInterval: 10000,
  });

  useEffect(() => {
    if (!selectedRunId && runsQuery.data?.runs?.[0]?.run_id) {
      setSelectedRun(String(runsQuery.data.runs[0].run_id));
    }
  }, [runsQuery.data, selectedRunId, setSelectedRun]);

  const detailsQuery = useQuery({
    queryKey: ["run-details", selectedRunId],
    queryFn: () => fetchJson<{ run: AnyRecord; stages: AnyRecord[]; alerts: AnyRecord[]; delivery_logs: AnyRecord[] }>(`/api/execution/runs/${selectedRunId}`),
    enabled: Boolean(selectedRunId),
  });

  return (
    <div className="page-grid">
      <Panel title="Recent Runs" subtitle="Select a run to inspect alerts, stage attempts, and publish outcomes" className="span-2">
        <DataTable rows={runsQuery.data?.runs ?? []} onRowClick={(row) => setSelectedRun(String(row.run_id ?? ""))} />
      </Panel>
      <Panel title="Run Summary" subtitle={selectedRunId ?? "No run selected"}>
        <KeyValueGrid
          items={[
            ["Run ID", detailsQuery.data?.run?.run_id],
            ["Status", detailsQuery.data?.run?.status],
            ["Stage", detailsQuery.data?.run?.current_stage],
            ["Run Date", detailsQuery.data?.run?.run_date],
            ["Error", detailsQuery.data?.run?.error_message],
          ]}
        />
      </Panel>
      <Panel title="Stage Attempts" subtitle="Stage-level execution history">
        <DataTable rows={detailsQuery.data?.stages ?? []} />
      </Panel>
      <Panel title="Alerts" subtitle="Pipeline and trust alerts for the selected run">
        <DataTable rows={detailsQuery.data?.alerts ?? []} />
      </Panel>
      <Panel title="Publish Logs" subtitle="Publisher delivery and retry information">
        <DataTable rows={detailsQuery.data?.delivery_logs ?? []} />
      </Panel>
    </div>
  );
}

function ShadowPage() {
  const shadowQuery = useQuery({
    queryKey: ["shadow"],
    queryFn: () => fetchJson<{ overlay: AnyRecord[]; weekly_5d: AnyRecord[]; weekly_20d: AnyRecord[]; monthly_5d: AnyRecord[]; monthly_20d: AnyRecord[] }>("/api/execution/shadow"),
    refetchInterval: 10000,
  });
  return (
    <div className="page-grid">
      <Panel title="Latest Overlay" subtitle="Champion vs challenger overlay snapshot" className="span-2">
        <DataTable rows={shadowQuery.data?.overlay ?? []} />
      </Panel>
      <Panel title="5D Weekly" subtitle="Weekly challenger summary">
        <DataTable rows={shadowQuery.data?.weekly_5d ?? []} />
      </Panel>
      <Panel title="20D Weekly" subtitle="Weekly challenger summary">
        <DataTable rows={shadowQuery.data?.weekly_20d ?? []} />
      </Panel>
      <Panel title="5D Monthly" subtitle="Monthly challenger summary">
        <DataTable rows={shadowQuery.data?.monthly_5d ?? []} />
      </Panel>
      <Panel title="20D Monthly" subtitle="Monthly challenger summary">
        <DataTable rows={shadowQuery.data?.monthly_20d ?? []} />
      </Panel>
    </div>
  );
}

function TasksPage() {
  const selectTask = useExecutionUiStore((state) => state.selectTask);
  const showToast = useExecutionUiStore((state) => state.showToast);
  const queryClient = useQueryClient();
  const tasksQuery = useQuery({
    queryKey: ["tasks"],
    queryFn: () => fetchJson<TaskListResponse>("/api/execution/tasks?limit=50"),
    refetchInterval: 5000,
  });
  const terminateTask = useMutation({
    mutationFn: (taskId: string) => postJson<AnyRecord>(`/api/execution/tasks/${taskId}/terminate`),
    onSuccess: (result, taskId) => {
      showToast("Task Action", String(result.message ?? `Task ${taskId} updated.`));
      void queryClient.invalidateQueries({ queryKey: ["tasks"] });
    },
    onError: (error, taskId) => {
      showToast("Task Action", `Task ${taskId} action failed: ${error instanceof Error ? error.message : "Unknown error"}`);
    },
  });
  return (
    <div className="page-grid">
      <Panel title="Operator Tasks" subtitle="Durable task history shared by NiceGUI and React" className="span-3">
        <DataTable
          rows={tasksQuery.data?.tasks ?? []}
          onRowClick={(row) => selectTask(String(row.task_id ?? ""))}
          getActionLabel={(row) => {
            const status = String(row.status ?? "").toLowerCase();
            return status === "running" ? "Terminate" : null;
          }}
          onAction={(row) => terminateTask.mutate(String(row.task_id ?? ""))}
        />
      </Panel>
    </div>
  );
}

function ProcessesPage() {
  const queryClient = useQueryClient();
  const processesQuery = useQuery({
    queryKey: ["processes"],
    queryFn: () => fetchJson<{ processes: AnyRecord[] }>("/api/execution/processes"),
    refetchInterval: 5000,
  });
  const terminate = useMutation({
    mutationFn: (pid: number) => postJson<AnyRecord>(`/api/execution/processes/${pid}/terminate`),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["processes"] }),
  });
  return (
    <div className="page-grid">
      <Panel title="Project Processes" subtitle="Execution UI, Streamlit, pipeline, and monitor processes" className="span-3">
        <DataTable rows={processesQuery.data?.processes ?? []} actionLabel="Terminate" onAction={(row) => terminate.mutate(Number(row.pid))} />
      </Panel>
    </div>
  );
}

function TaskDrawer() {
  const selectedTaskId = useExecutionUiStore((state) => state.selectedTaskId);
  const isOpen = useExecutionUiStore((state) => state.isTaskDrawerOpen);
  const closeTaskDrawer = useExecutionUiStore((state) => state.closeTaskDrawer);
  const { taskState, logs, cursor } = useLiveTask(selectedTaskId);

  if (!isOpen) return null;

  return (
    <aside className="task-drawer">
      <div className="drawer-header">
        <div>
          <div className="eyebrow">Live Task Stream</div>
          <h3>{selectedTaskId ?? "No task selected"}</h3>
          <p>{String(taskState?.label ?? "Select a task from the Tasks view or progress center.")}</p>
        </div>
        <button className="ghost-button" onClick={closeTaskDrawer}>
          Close
        </button>
      </div>
      <KeyValueGrid
        items={[
          ["Status", taskState?.status],
          ["Type", taskState?.task_type],
          ["Current Step", taskState?.current_stage_label ?? taskState?.phase_label],
          ["Run ID", taskState?.run_id],
          ["Started", taskState?.started_at],
          ["Cursor", cursor],
        ]}
      />
      <StageTimeline rows={(taskState?.stage_statuses as AnyRecord[]) ?? []} />
      <PublishProgressPanel publishProgress={taskState?.publish_progress as AnyRecord | undefined} />
      <div className="log-stream">
        {logs.length === 0 ? <div className="empty-state">Waiting for task log events…</div> : logs.map((line, index) => <pre key={`${index}-${line}`}>{line}</pre>)}
      </div>
    </aside>
  );
}

function PublishProgressPanel(props: { publishProgress?: AnyRecord }) {
  const progress = props.publishProgress;
  const channels = Array.isArray(progress?.channels) ? (progress?.channels as AnyRecord[]) : [];
  const summary = (progress?.summary as AnyRecord | undefined) ?? {};
  if (!channels.length) {
    return null;
  }

  return (
    <section className="publish-progress">
      <div className="panel-header">
        <div>
          <div className="eyebrow">Publish Progress</div>
          <p>
            Delivered {String(summary.delivered ?? 0)} / {String(summary.total ?? channels.length)} channels
          </p>
        </div>
      </div>
      <div className="publish-summary-grid">
        <div className="brief-block">
          <span>Delivered</span>
          <strong>{String(summary.delivered ?? 0)}</strong>
        </div>
        <div className="brief-block">
          <span>Retrying</span>
          <strong>{String(summary.retrying ?? 0)}</strong>
        </div>
        <div className="brief-block">
          <span>Failed</span>
          <strong>{String(summary.failed ?? 0)}</strong>
        </div>
        <div className="brief-block">
          <span>Pending</span>
          <strong>{String(summary.pending ?? 0)}</strong>
        </div>
      </div>
      <div className="publish-channel-grid">
        {channels.map((row) => (
          <div key={String(row.channel ?? row.label)} className={`publish-channel-card publish-${String(row.status ?? "pending").toLowerCase()}`}>
            <div className="publish-channel-header">
              <strong>{String(row.label ?? row.channel ?? "Channel")}</strong>
              <span className={`status-pill status-${String(row.status ?? "pending").toLowerCase()}`}>{String(row.status ?? "pending").toUpperCase()}</span>
            </div>
            <p>{String(row.detail ?? "No delivery detail yet.")}</p>
            <span className="publish-attempt">Attempt {String(row.attempt_number ?? "—")}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function MetricCard(props: { title: string; value: string | number; detail: string; tone: string }) {
  return (
    <section className={`metric-card metric-${props.tone}`}>
      <div className="eyebrow">{props.title}</div>
      <div className="metric-value">{props.value}</div>
      <p>{props.detail}</p>
    </section>
  );
}

function OpsHealthRibbon(props: {
  snapshot?: PipelineWorkspaceResponse["ops_health"];
  health?: PipelineWorkspaceResponse["health"];
  trust?: AnyRecord;
}) {
  const stages = props.snapshot?.stages ? Object.values(props.snapshot.stages) : [];
  const dqSummary = props.snapshot?.dq_summary ?? {};
  const healthSummary = props.health?.summary ?? {};
  const trust = props.trust ?? {};
  const trustStatus = String(trust?.status ?? "—");
  const trustFallbackRatio = Number(trust?.fallback_ratio_latest ?? 0) * 100;

  const normalizeTone = (value: string): string => {
    const tone = value.toLowerCase();
    if (tone === "degraded" || tone === "stale") return "warn";
    if (tone === "failed") return "error";
    if (tone === "fresh" || tone === "passed" || tone === "ok") return "ok";
    return tone;
  };

  const cards = [
    ...stages.map((stage) => ({
      title: String(stage.stage_name ?? "stage").toUpperCase(),
      status: Boolean(stage.stale) ? "STALE" : "FRESH",
      detail: String(stage.run_id ?? "—"),
      subdetail: typeof stage.age_hours === "number" ? `${Number(stage.age_hours).toFixed(1)}h ago` : "n/a",
      tone: Boolean(stage.stale) ? "warn" : "ok",
    })),
    {
      title: "DQ",
      status: Number(dqSummary.total_failed ?? 0) > 0 ? "FAILED" : "PASSED",
      detail: String(dqSummary.total_failed ?? 0),
      subdetail: Object.keys((dqSummary.failed_by_severity as AnyRecord | undefined) ?? {}).length
        ? Object.entries((dqSummary.failed_by_severity as AnyRecord) ?? {}).map(([key, value]) => `${key}:${value}`).join(", ")
        : "none",
      tone: Number(dqSummary.total_failed ?? 0) > 0 ? "error" : "ok",
    },
    {
      title: "PIPELINE",
      status: String(props.health?.status?.toUpperCase() ?? "—"),
      detail: `OHLCV ${String(healthSummary?.latest_ohlcv_date ?? "—")}`,
      subdetail: `Delivery ${String(healthSummary?.latest_delivery_date ?? "—")} · Payload ${String(healthSummary?.payload_age_minutes ?? "—")}m`,
      tone: normalizeTone(String(props.health?.status ?? "warn")),
    },
    {
      title: "TRUST",
      status: trustStatus.toUpperCase(),
      detail: `Validated ${String(trust?.latest_validated_date ?? "—")}`,
      subdetail: `Fallback ${trustFallbackRatio.toFixed(1)}% · Q ${String(trust?.active_quarantined_symbols ?? "0")}`,
      tone: normalizeTone(trustStatus),
    },
  ];

  return (
    <div className="ops-ribbon-react">
      {cards.map((card) => (
        <article key={`${card.title}-${card.detail}`} className={`ops-ribbon-react-card tone-${card.tone}`}>
          <div className="ops-ribbon-react-title">{card.title}</div>
          <div className="ops-ribbon-react-main">
            <span className={`status-pill status-${card.tone}`}>{card.status}</span>
            <strong>{card.detail}</strong>
          </div>
          <div className="ops-ribbon-react-sub">{card.subdetail}</div>
        </article>
      ))}
    </div>
  );
}

function Panel(props: { title: string; subtitle?: string; className?: string; children: React.ReactNode }) {
  return (
    <section className={`panel ${props.className ?? ""}`}>
      <div className="panel-header">
        <div>
          <div className="eyebrow">{props.title}</div>
          {props.subtitle ? <p>{props.subtitle}</p> : null}
        </div>
      </div>
      {props.children}
    </section>
  );
}

function KeyValueGrid(props: { items: Array<[string, unknown]> }) {
  return (
    <div className="kv-grid">
      {props.items.map(([label, value]) => (
        <div key={label} className="kv-item">
          <span>{label}</span>
          <strong>{String(value ?? "—")}</strong>
        </div>
      ))}
    </div>
  );
}

function ChartCard(props: { rows: AnyRecord[]; xKey: string; yKey: string }) {
  if (!props.rows.length) {
    return <div className="empty-state">No chart data available.</div>;
  }
  return (
    <div className="chart-shell">
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={props.rows}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(120,140,180,0.16)" />
          <XAxis dataKey={props.xKey} stroke="#8ea2c1" />
          <YAxis stroke="#8ea2c1" />
          <Tooltip />
          <Bar dataKey={props.yKey} fill="#34d399" radius={[10, 10, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function DataTable(props: {
  rows: AnyRecord[];
  onRowClick?: (row: AnyRecord) => void;
  actionLabel?: string;
  getActionLabel?: (row: AnyRecord) => string | null;
  onAction?: (row: AnyRecord) => void;
}) {
  const rows = props.rows;
  const columns = useMemo(() => {
    const keys = new Set<string>();
    rows.forEach((row) => Object.keys(row).slice(0, 12).forEach((key) => keys.add(key)));
    return Array.from(keys);
  }, [rows]);

  if (!rows.length) {
    return <div className="empty-state">No data available.</div>;
  }

  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
            {props.actionLabel ? <th>Action</th> : null}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={`${rowIndex}-${String(row[columns[0]] ?? rowIndex)}`} onClick={() => props.onRowClick?.(row)}>
              {columns.map((column) => (
                <td key={column}>{formatCell(row[column])}</td>
              ))}
              {(props.actionLabel || props.getActionLabel) ? (
                <td>
                  {(() => {
                    const actionText = props.getActionLabel ? props.getActionLabel(row) : props.actionLabel ?? null;
                    if (!actionText) {
                      return <span className="muted-cell">—</span>;
                    }
                    return (
                      <button
                        className="inline-button"
                        onClick={(event) => {
                          event.stopPropagation();
                          props.onAction?.(row);
                        }}
                      >
                        {actionText}
                      </button>
                    );
                  })()}
                </td>
              ) : null}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export default App;
