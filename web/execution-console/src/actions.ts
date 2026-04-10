export type ExecutionActionId =
  | "full_pipeline"
  | "market_refresh"
  | "publish_retry"
  | "shadow_refresh"
  | "open_research";

export type ExecutionActionDefinition = {
  id: ExecutionActionId;
  label: string;
  shortLabel: string;
  summary: string;
  purpose: string;
  stageLabels: string[];
  publishIncluded: boolean;
  expectedOutputs: string[];
  riskNote: string;
  estimatedDuration: string;
  sideEffectLevel: "publish" | "background" | "local";
  badges: string[];
  apiPath: string;
  requestBody: Record<string, unknown>;
};

export const EXECUTION_ACTIONS: ExecutionActionDefinition[] = [
  {
    id: "full_pipeline",
    label: "Run Full Pipeline",
    shortLabel: "Ingest + features + rank + publish",
    summary: "Runs the complete operational pipeline and pushes successful publish outputs downstream.",
    purpose: "Refresh live market state end to end, then attempt delivery to publish channels.",
    stageLabels: ["Updating market data", "Computing indicators", "Refreshing rankings", "Publishing outputs"],
    publishIncluded: true,
    expectedOutputs: ["Operational OHLC and delivery refresh", "Feature snapshot", "Rank artifacts + dashboard payload", "Telegram / Sheets / tear-sheet publish"],
    riskNote: "May stop on DQ or trust failures. Successful publish can send external notifications.",
    estimatedDuration: "2-8 minutes",
    sideEffectLevel: "publish",
    badges: ["Publishes externally", "Background job"],
    apiPath: "/api/execution/pipeline/run",
    requestBody: {
      label: "Full operational pipeline",
      stages: ["ingest", "features", "rank", "publish"],
      params: {
        data_domain: "operational",
        preflight: true,
        local_publish: false,
        full_rebuild: false,
        feature_tail_bars: 252,
        symbol_limit: 25,
      },
    },
  },
  {
    id: "market_refresh",
    label: "Market Refresh",
    shortLabel: "Ingest + features + rank",
    summary: "Refreshes data and rankings without running publish.",
    purpose: "Bring market data, indicators, and ranking artifacts up to date with no external publish side effects.",
    stageLabels: ["Updating market data", "Computing indicators", "Refreshing rankings"],
    publishIncluded: false,
    expectedOutputs: ["Operational OHLC and delivery refresh", "Feature snapshot", "Rank artifacts + dashboard payload"],
    riskNote: "No publish side effects. Still subject to DQ and trust gates.",
    estimatedDuration: "2-6 minutes",
    sideEffectLevel: "background",
    badges: ["No publish", "Background job"],
    apiPath: "/api/execution/pipeline/run",
    requestBody: {
      label: "Market refresh",
      stages: ["ingest", "features", "rank"],
      params: {
        data_domain: "operational",
        preflight: true,
        local_publish: false,
        full_rebuild: false,
        feature_tail_bars: 252,
        symbol_limit: 25,
      },
    },
  },
  {
    id: "publish_retry",
    label: "Publish Retry",
    shortLabel: "Publish only",
    summary: "Retries delivery for the most recent operational run.",
    purpose: "Reattempt Telegram, Google Sheets, and tear-sheet publish without recomputing market data.",
    stageLabels: ["Publishing outputs"],
    publishIncluded: true,
    expectedOutputs: ["Publisher delivery retry", "Updated publish logs", "Possible Telegram / Sheets resend"],
    riskNote: "External delivery only. Can resend outputs if the prior attempt partly succeeded.",
    estimatedDuration: "10-60 seconds",
    sideEffectLevel: "publish",
    badges: ["Publishes externally", "Background job"],
    apiPath: "/api/execution/pipeline/publish-retry",
    requestBody: {
      local_publish: false,
    },
  },
  {
    id: "shadow_refresh",
    label: "Shadow Refresh",
    shortLabel: "Shadow overlay only",
    summary: "Refreshes the ML challenger shadow overlay and summary tables.",
    purpose: "Update champion-vs-challenger monitoring without changing the core market ranking pipeline.",
    stageLabels: ["Refreshing overlay", "Recomputing shadow summaries"],
    publishIncluded: false,
    expectedOutputs: ["Latest overlay snapshot", "Weekly challenger summaries", "Monthly challenger summaries"],
    riskNote: "No core market ranking or publish side effects.",
    estimatedDuration: "30-90 seconds",
    sideEffectLevel: "background",
    badges: ["No publish", "Background job"],
    apiPath: "/api/execution/shadow/run",
    requestBody: {
      label: "Shadow refresh",
      backfill_days: 0,
    },
  },
  {
    id: "open_research",
    label: "Open Research",
    shortLabel: "Launch local research UI",
    summary: "Starts the Streamlit research dashboard process on the configured local port.",
    purpose: "Open the research surface for explainability, ranking inspection, and portfolio analysis.",
    stageLabels: ["Launching process"],
    publishIncluded: false,
    expectedOutputs: ["Background Streamlit process", "Local dashboard URL"],
    riskNote: "Starts a local background process only.",
    estimatedDuration: "5-15 seconds",
    sideEffectLevel: "local",
    badges: ["Starts process", "Local only"],
    apiPath: "/api/execution/research/launch",
    requestBody: {
      port: 8501,
    },
  },
];

export function getExecutionAction(actionId: ExecutionActionId): ExecutionActionDefinition {
  const action = EXECUTION_ACTIONS.find((item) => item.id === actionId);
  if (!action) {
    throw new Error(`Unknown execution action: ${actionId}`);
  }
  return action;
}
