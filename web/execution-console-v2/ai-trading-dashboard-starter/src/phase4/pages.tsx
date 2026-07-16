import { useMemo, useState } from 'react';
import { NavLink, useParams, useSearchParams } from 'react-router-dom';
import {
  ConflictBanner, DataTable, EmptyState, ErrorState, FilterBar, KeyValuePanel, LimitationList,
  PageHeader, PaginationControls, PartialDataBanner, ReadinessBadge, SeverityBadge,
  StatusBadge, Tabs, useDetailNavigation, type Column, type FilterOption,
} from './components';
import { usePhase4Query, withParams } from './queries';
import type { ApiEnvelope, JsonRecord, ResponseMeta } from './types';
import { asRecord, asRecords, text } from './types';

const statusColumn = (key: string, label: string): Column => ({ key, label, render: (value) => <StatusBadge value={value} /> });
const boolColumn = (key: string, label: string): Column => ({ key, label, render: (value) => <StatusBadge value={value === true ? 'Yes' : value === false ? 'No' : 'Unknown'} /> });
const listColumn = (key: string, label: string): Column => ({ key, label, render: (value) => Array.isArray(value) && value.length ? <ul className="inline-list">{value.map((item) => <li key={String(item)}>{text(item)}</li>)}</ul> : 'Unavailable' });
const options = (...values: string[]): FilterOption[] => values.map((value) => ({ value, label: value.replace(/_/g, ' ') }));
const STAGE_OPTIONS = options('unknown', 'stage_1_basing', 'transition_1_to_2', 'stage_2_advancing', 'transition_2_to_3', 'stage_3_topping', 'stage_4_declining');
const STAGE_STATUS_OPTIONS = options('provisional', 'locked', 'conflicted', 'unknown');
const CANDIDATE_STATE_OPTIONS = options('unseen', 'discovered', 'investigating', 'early_accumulation', 'setup_forming', 'ready', 'triggered', 'pending_followthrough', 'confirmed', 'advancing', 'extended', 'weakening', 'failed', 'exited', 'archived', 'open', 'closed');
const SETUP_FAMILY_OPTIONS = options('early_accumulation', 'base_building', 'stage_1_to_2_transition', 'breakout', 'post_breakout_followthrough', 'pullback_reentry', 'momentum_leader', 'manual', 'position_state_recovery');
const SCAN_REASON_OPTIONS = options('full_universe_structural', 'stage_1_discovery', 'stage_transition_discovery', 'rank_selected', 'stage_promoted', 'active_position', 'recent_exit', 'triggered_candidate', 'pending_followthrough', 'manual_override');

function QueryState({ query, children }: { query: { isLoading: boolean; error: unknown; data?: ApiEnvelope<unknown> }; children: (response: ApiEnvelope<unknown>) => React.ReactNode }) {
  if (query.isLoading) return <div className="loading" role="status">Loading governed evidence…</div>;
  if (query.error) return <ErrorState error={query.error} />;
  if (!query.data) return <EmptyState />;
  return <>{children(query.data)}</>;
}

function Metric({ label, value, unavailable = false }: { label: string; value: unknown; unavailable?: boolean }) {
  return <article className={`metric ${unavailable ? 'metric-unavailable' : ''}`}><span>{label}</span><strong>{unavailable ? 'Unavailable' : text(value)}</strong></article>;
}

function countBy(rows: JsonRecord[], key: string, value?: string): number {
  return value ? rows.filter((row) => String(row[key]).toLowerCase() === value.toLowerCase()).length : rows.length;
}

export function OverviewPage() {
  const readiness = usePhase4Query<JsonRecord>('/api/v1/system/readiness', { poll: true });
  const market = usePhase4Query<JsonRecord>('/api/v1/market/stage', { poll: true });
  const routing = usePhase4Query<JsonRecord[]>('/api/v1/routing?limit=500', { poll: true });
  const candidates = usePhase4Query<JsonRecord[]>('/api/v1/candidates?limit=500', { poll: true });
  const positions = usePhase4Query<JsonRecord[]>('/api/v1/positions/coverage', { poll: true });
  const alerts = usePhase4Query<JsonRecord[]>('/api/v1/alerts', { poll: true });
  const conflicts = usePhase4Query<JsonRecord[]>('/api/v1/governance/conflicts', { poll: true });
  const calibration = usePhase4Query<JsonRecord>('/api/v1/calibration/summary', { poll: true });
  const performance = usePhase4Query<JsonRecord>('/api/v1/performance/latest', { poll: true });
  if (readiness.isLoading) return <div className="loading">Loading operator overview…</div>;
  if (readiness.error) return <ErrorState error={readiness.error} />;
  const ready = asRecord(readiness.data?.data);
  const routeRows = asRecords(routing.data?.data);
  const candidateRows = asRecords(candidates.data?.data);
  const positionRows = asRecords(positions.data?.data);
  const alertRows = asRecords(alerts.data?.data);
  const conflictRows = asRecords(conflicts.data?.data);
  const calibrationData = asRecord(calibration.data?.data);
  const marketData = asRecord(market.data?.data);
  const observations = asRecords(marketData.observations);
  return <div><PageHeader eyebrow="Operational overview" title="What needs attention now" description="API-projected status only. No strategy, ranking, routing, or lifecycle logic is recomputed here." meta={readiness.data?.meta} />
    <section className="summary-grid" aria-label="Readiness"><Metric label="Formal verdict" value={ready.readiness_status} /><Metric label="Development ready" value={ready.phase4_development_ready} /><Metric label="Production ready" value={ready.phase4_production_ready} /><Metric label="Active limitations" value={Array.isArray(ready.limitations) ? ready.limitations.length : 'Unavailable'} /></section>
    <section className="section-card"><h2>Production blockers</h2><LimitationList limitations={Array.isArray(ready.limitations) ? ready.limitations.map((item) => text(asRecord(item).limitation_id, '')).filter(Boolean) : []} /></section>
    <div className="overview-grid">
      <section className="section-card"><h2>Market state</h2><Metric label="Available observations" value={observations.length} /><Metric label="Latest market session" value={market.data?.meta.freshness.latest_market_session} /><Metric label="Governance conflicts" value={asRecords(marketData.conflicts).length} />{market.error ? <ErrorState error={market.error} /> : null}</section>
      <section className="section-card"><h2>Routing</h2><Metric label="Available routed symbols" value={routing.error ? null : routeRows.length} unavailable={Boolean(routing.error)} /><Metric label="Position monitor" value={countBy(routeRows, 'effective_scan_tier', 'position_monitor')} /><Metric label="Structurally blocked new longs" value={routeRows.filter((row) => row.new_long_structural_block === true).length} /><Metric label="Active-position structural risk" value={routeRows.filter((row) => row.active_position_structural_risk === true).length} /></section>
      <section className="section-card"><h2>Candidates</h2><Metric label="Available episodes" value={candidates.error ? null : candidateRows.length} unavailable={Boolean(candidates.error)} /><Metric label="Pending follow-through" value={countBy(candidateRows, 'candidate_state', 'pending_followthrough')} /><Metric label="Incomplete history" value={candidateRows.filter((row) => String(row.history_completeness).toUpperCase() !== 'COMPLETE').length} /></section>
      <section className="section-card"><h2>Position monitoring</h2><Metric label="Active positions" value={positions.error ? null : positionRows.length} unavailable={Boolean(positions.error)} /><Metric label="Fully monitored" value={countBy(positionRows, 'coverage_status', 'FULLY_MONITORED')} /><Metric label="Missing routing" value={countBy(positionRows, 'coverage_status', 'MISSING_ROUTING')} /><Metric label="Positive action suppressed" value={positionRows.filter((row) => row.positive_action_suppressed === true).length} /></section>
      <section className="section-card"><h2>Alerts</h2><Metric label="Open critical" value={alertRows.filter((row) => String(row.status).toLowerCase() === 'open' && String(row.severity).toLowerCase() === 'critical').length} /><Metric label="Open high" value={alertRows.filter((row) => String(row.status).toLowerCase() === 'open' && String(row.severity).toLowerCase() === 'high').length} /><Metric label="Recurred incidents" value={countBy(alertRows, 'status', 'recurred')} /></section>
      <section className="section-card"><h2>Quality and readiness</h2><Metric label="Governance conflicts" value={conflicts.error ? null : conflictRows.length} unavailable={Boolean(conflicts.error)} /><Metric label="Calibration exclusions" value={calibrationData.excluded_count} unavailable={Boolean(calibration.error)} /><Metric label="Calibration quarantines" value={calibrationData.quarantined_count} unavailable={Boolean(calibration.error)} /><Metric label="Latest performance" value={asRecord(performance.data?.data).performance_status} unavailable={Boolean(performance.error)} /></section>
    </div>
  </div>;
}

export function MarketPage() {
  const [params] = useSearchParams();
  const asOf = params.get('as_of');
  const suffix = asOf ? `?as_of=${encodeURIComponent(asOf)}` : '';
  const market = usePhase4Query<JsonRecord>(`/api/v1/market/stage${suffix}`);
  const sectors = usePhase4Query<JsonRecord[]>(withParams('/api/v1/sectors', params, ['as_of', 'sector_stage', 'stage_status']));
  const symbol = params.get('symbol')?.trim() ?? '';
  const stocks = usePhase4Query<JsonRecord[]>(withParams('/api/v1/stocks', params, ['as_of', 'symbol', 'sector', 'stock_stage', 'stage_status']), { enabled: Boolean(symbol) });
  const sectorColumns: Column[] = [{ key: 'sector_name', label: 'Sector' }, statusColumn('effective_stage', 'Current stage'), statusColumn('stage_status', 'Stage status'), { key: 'stage_confidence', label: 'Confidence' }, statusColumn('membership_trust', 'Membership trust'), { key: 'source_week_end', label: 'Source session' }, { key: 'available_at', label: 'Available at' }, statusColumn('governance_status', 'Governance')];
  const stockColumns: Column[] = [{ key: 'symbol_id', label: 'Symbol' }, { key: 'exchange', label: 'Exchange' }, { key: 'sector_name', label: 'Sector' }, statusColumn('effective_stage', 'Stock stage'), statusColumn('stage_status', 'Status'), { key: 'stage_confidence', label: 'Confidence' }, statusColumn('sector_stage', 'Sector stage'), statusColumn('governance_status', 'Governance')];
  return <div><PageHeader eyebrow="Market structure" title="Market, sectors, and stock stages" description="Historical stage selection is delegated to the API. Conflicted entities never receive a frontend-selected winner." meta={market.data?.meta ?? sectors.data?.meta} asOf />
    <section className="section-card"><h2>Market-stage summary</h2><p className="muted">Search and page through the latest governed stock-stage observations.</p><QueryState query={market}>{(response) => { const data = asRecord(response.data); const conflicts = asRecords(data.conflicts); return <>{conflicts.length ? <ConflictBanner message={`${conflicts.length} governed conflicts are present; no false authoritative value is shown.`} /> : null}<DataTable rows={asRecords(data.observations)} columns={[{ key: 'symbol_id', label: 'Symbol' }, statusColumn('effective_stage', 'Stage'), statusColumn('stage_status', 'Status'), { key: 'stage_confidence', label: 'Confidence' }, { key: 'as_of', label: 'As of' }]} rowKey={(row, index) => text(row.observation_id, String(index))} caption="Market stage observations" clientPageSize={25} /></>; }}</QueryState></section>
    <section className="section-card"><h2>Sector stages</h2><p className="muted">Use governed stage and status values; filters are sent to the API and remain in the URL.</p><FilterBar title="Filter sectors" filters={[{ key: 'sector_stage', label: 'Stage', kind: 'select', options: STAGE_OPTIONS }, { key: 'stage_status', label: 'Status', kind: 'select', options: STAGE_STATUS_OPTIONS }]} /><QueryState query={sectors}>{(response) => <><PartialDataBanner meta={response.meta} /><DataTable rows={asRecords(response.data)} columns={sectorColumns} rowKey={(row, index) => text(row.observation_id, String(index))} caption="Sector stages" clientPageSize={25} /></>}</QueryState></section>
    <section className="section-card"><h2>Stock stage explorer</h2><p className="muted">Enter a symbol to query the server. The browser does not load the full universe.</p><FilterBar title="Find a stock" filters={[{ key: 'symbol', label: 'Symbol', kind: 'text', placeholder: 'e.g. RELIANCE', uppercase: true }, { key: 'stock_stage', label: 'Stock stage', kind: 'select', options: STAGE_OPTIONS }, { key: 'stage_status', label: 'Status', kind: 'select', options: STAGE_STATUS_OPTIONS }]} />{symbol ? <QueryState query={stocks}>{(response) => <DataTable rows={asRecords(response.data)} columns={stockColumns} rowKey={(row, index) => text(row.observation_id, String(index))} caption="Stock stages" />}</QueryState> : <EmptyState title="Search for a stock" detail="Symbol-scoped requests preserve the API-only boundary and avoid loading all symbols into browser memory." />}</section>
  </div>;
}

interface CollectionPageProps { title: string; eyebrow: string; description: string; path: string; allowed: string[]; columns: Column[]; rowId: string; detailPrefix?: string; poll?: boolean; serverPagination?: boolean; controls?: React.ReactNode }
function CollectionPage({ title, eyebrow, description, path, allowed, columns, rowId, detailPrefix, poll, serverPagination, controls }: CollectionPageProps) {
  const [params, setParams] = useSearchParams();
  const [history, setHistory] = useState<string[]>([]);
  const requestPath = withParams(path, params, allowed);
  const query = usePhase4Query<JsonRecord[]>(requestPath, { poll });
  const goDetail = useDetailNavigation(detailPrefix ?? '', rowId);
  const sortFields = rowId === 'decision_id' ? ['as_of', 'symbol_id', 'decision_id'] : rowId === 'candidate_id' ? ['opened_at', 'symbol_id', 'candidate_id'] : [];
  const sortControls = serverPagination && sortFields.length ? <div className="sort-controls"><label>Server sort<select value={params.get('sort') ?? sortFields[0]} onChange={(event) => { const next = new URLSearchParams(params); next.set('sort', event.target.value); next.delete('cursor'); setHistory([]); setParams(next); }}>{sortFields.map((field) => <option key={field} value={field}>{field.replace(/_/g, ' ')}</option>)}</select></label><label>Order<select value={params.get('order') ?? 'desc'} onChange={(event) => { const next = new URLSearchParams(params); next.set('order', event.target.value); next.delete('cursor'); setHistory([]); setParams(next); }}><option value="desc">Descending</option><option value="asc">Ascending</option></select></label></div> : null;
  return <div><PageHeader eyebrow={eyebrow} title={title} description={description} meta={query.data?.meta} />{controls}{sortControls}<QueryState query={query}>{(response) => <><PartialDataBanner meta={response.meta} /><DataTable rows={asRecords(response.data)} columns={columns} rowKey={(row, index) => text(row[rowId], String(index))} caption={title} onRow={detailPrefix ? goDetail : undefined} /><PaginationControls meta={serverPagination ? response.meta : undefined} canPrevious={history.length > 0} onPrevious={() => { const previous = history[history.length - 1]; const next = new URLSearchParams(params); previous ? next.set('cursor', previous) : next.delete('cursor'); setHistory(history.slice(0, -1)); setParams(next); }} onNext={() => { const cursor = response.meta.pagination?.next_cursor; if (!cursor) return; setHistory([...history, params.get('cursor') ?? '']); const next = new URLSearchParams(params); next.set('cursor', cursor); setParams(next); }} /></>}</QueryState></div>;
}

export function RoutingPage() {
  const [params, setParams] = useSearchParams();
  const columns: Column[] = [{ key: 'symbol_id', label: 'Symbol' }, { key: 'exchange', label: 'Exchange' }, statusColumn('effective_scan_tier', 'Effective tier'), { key: 'winning_reason', label: 'Winning reason', render: (value) => <strong>{text(value)}</strong> }, listColumn('all_reasons', 'All retained reasons'), boolColumn('new_long_structural_block', 'New-long block'), boolColumn('active_position_structural_risk', 'Position risk'), { key: 'risk_severity', label: 'Risk severity', render: (value) => <SeverityBadge value={value} /> }, { key: 'policy_version', label: 'Policy' }, { key: 'decision_id', label: 'Decision ID' }];
  const tabs = [['all', ''], ['position monitor', 'position_monitor'], ['full investigator', 'full_investigator'], ['light pattern', 'light_pattern'], ['stage only', 'stage_only']] as const;
  return <CollectionPage title="Routing decisions" eyebrow="Scan routing" description="Winning reasons remain prominent while every retained non-winning reason stays visible." path="/api/v1/routing" allowed={['limit', 'cursor', 'sort', 'order', 'symbol', 'scan_tier', 'scan_reason']} columns={columns} rowId="decision_id" detailPrefix="/routing" serverPagination controls={<><div className="tabs">{tabs.map(([label, value]) => <button key={label} type="button" aria-pressed={(params.get('scan_tier') ?? '') === value} onClick={() => { const next = new URLSearchParams(params); value ? next.set('scan_tier', value) : next.delete('scan_tier'); next.delete('cursor'); setParams(next); }}>{label}</button>)}<NavLink to="/governance?tab=routing">Routing conflicts</NavLink></div><FilterBar title="Filter routing" filters={[{ key: 'symbol', label: 'Symbol', kind: 'text', uppercase: true }, { key: 'scan_reason', label: 'Winning reason', kind: 'select', options: SCAN_REASON_OPTIONS }]} /></>} />;
}

export function CandidatesPage() {
  const columns: Column[] = [{ key: 'candidate_id', label: 'Candidate ID' }, { key: 'symbol_id', label: 'Symbol' }, { key: 'exchange', label: 'Exchange' }, { key: 'setup_family', label: 'Setup family' }, statusColumn('candidate_state', 'Lifecycle state'), { key: 'opened_at', label: 'Opened' }, statusColumn('followthrough_status', 'Follow-through'), statusColumn('history_completeness', 'History'), boolColumn('recovered_from_position_state', 'Recovered')];
  const filters = <FilterBar title="Filter candidates" filters={[{ key: 'symbol', label: 'Symbol', kind: 'text', uppercase: true }, { key: 'candidate_state', label: 'Lifecycle state', kind: 'select', options: CANDIDATE_STATE_OPTIONS }, { key: 'setup_family', label: 'Setup family', kind: 'select', options: SETUP_FAMILY_OPTIONS }]} />;
  return <CollectionPage title="Candidate lifecycle" eyebrow="Candidate registry" description="Episodes, follow-through, and recovered-history limitations are rendered from API evidence only." path="/api/v1/candidates" allowed={['limit', 'cursor', 'sort', 'order', 'symbol', 'candidate_state', 'setup_family']} columns={columns} rowId="candidate_id" detailPrefix="/candidates" serverPagination controls={filters} />;
}

export function PositionsPage() {
  const [params] = useSearchParams();
  const tab = params.get('tab') ?? 'coverage';
  const configs: Record<string, { path: string; columns: Column[]; rowId: string }> = {
    coverage: { path: withParams('/api/v1/positions/coverage', params, ['coverage_status']), rowId: 'position_cycle_id', columns: [{ key: 'symbol_id', label: 'Symbol' }, { key: 'position_cycle_id', label: 'Position cycle' }, statusColumn('coverage_status', 'Coverage'), statusColumn('effective_scan_tier', 'Scan tier'), boolColumn('market_data_complete', 'Market data complete'), boolColumn('evidence_complete', 'Evidence complete'), listColumn('missing_fields', 'Missing fields'), statusColumn('episode_compatibility', 'Episode compatibility'), boolColumn('positive_action_suppressed', 'Positive action suppressed'), statusColumn('recovery_status', 'Recovery')] },
    'missing-data': { path: '/api/v1/positions/missing-data', rowId: 'position_cycle_id', columns: [{ key: 'symbol_id', label: 'Symbol' }, { key: 'position_cycle_id', label: 'Position cycle' }, listColumn('missing_data_fields', 'Missing fields'), { key: 'staleness_sessions', label: 'Staleness sessions' }, { key: 'last_valid_session', label: 'Last valid session' }, { key: 'expected_session', label: 'Expected session' }, statusColumn('coverage_status', 'Alert status')] },
    recovery: { path: '/api/v1/positions/recovery-proposals', rowId: 'recovery_proposal_id', columns: [{ key: 'recovery_proposal_id', label: 'Proposal ID' }, { key: 'symbol_id', label: 'Symbol' }, { key: 'position_cycle_id', label: 'Position cycle' }, statusColumn('recovery_mode', 'Recovery mode'), statusColumn('proposal_status', 'Proposal status'), statusColumn('compatibility_status', 'Compatibility'), boolColumn('pre_entry_history_available', 'Pre-entry history'), statusColumn('action_status', 'Action status')] },
  };
  const config = configs[tab] ?? configs.coverage;
  const query = usePhase4Query<JsonRecord[]>(config.path, { poll: true });
  const goDetail = useDetailNavigation('/positions', 'position_cycle_id');
  return <div><PageHeader eyebrow="Position safety" title="Active-position monitoring" description="Coverage gaps, positive-action suppression, episode compatibility, and recovery proposals are read-only." meta={query.data?.meta} /><Tabs options={['coverage', 'missing-data', 'recovery']} />{tab === 'coverage' ? <FilterBar title="Filter position coverage" filters={[{ key: 'coverage_status', label: 'Coverage status', kind: 'select', options: options('fully_monitored', 'routed_with_incomplete_data', 'missing_routing', 'incompatible_episode', 'recovery_required', 'hard_exclusion') }]} /> : null}<QueryState query={query}>{(response) => <DataTable rows={asRecords(response.data)} columns={config.columns} rowKey={(row, index) => text(row[config.rowId], String(index))} caption="Position monitoring" onRow={tab === 'coverage' ? goDetail : undefined} />}</QueryState></div>;
}

export function AlertsPage() {
  const [params] = useSearchParams();
  const tab = params.get('tab') ?? 'alerts';
  const path = tab === 'incidents' ? '/api/v1/alert-incidents' : '/api/v1/alerts';
  const columns: Column[] = [{ key: 'alert_code', label: 'Alert code' }, { key: 'severity', label: 'Severity', render: (value) => <SeverityBadge value={value} /> }, statusColumn('status', 'Status'), { key: 'symbol_id', label: 'Symbol' }, { key: 'position_cycle_id', label: 'Position cycle' }, { key: 'opened_at', label: 'Opened' }, { key: 'resolved_at', label: 'Resolved' }, { key: 'occurrence_count', label: 'Occurrences' }, { key: 'recommended_operator_action', label: 'Recommended operator action' }];
  return <CollectionPage title="Alerts and incidents" eyebrow="Operational attention" description="Open, resolved, and recurred incident evidence. No acknowledgement or resolution controls are present." path={path} allowed={['severity', 'status']} columns={columns} rowId="alert_id" detailPrefix={tab === 'incidents' ? '/incidents' : '/alerts'} poll controls={<><Tabs options={['alerts', 'incidents']} /><FilterBar title="Filter alerts" filters={[{ key: 'severity', label: 'Severity', kind: 'select', options: options('info', 'warning', 'critical') }, { key: 'status', label: 'Status', kind: 'select', options: options('open', 'resolved', 'recurred') }]} /></>} />;
}

export function GovernancePage() {
  const [params] = useSearchParams();
  const tab = params.get('tab') ?? 'conflicts';
  const configs: Record<string, [string, Column[], string]> = {
    conflicts: ['/api/v1/governance/conflicts', [{ key: 'conflict_type', label: 'Conflict type' }, { key: 'conflict_code', label: 'Conflict code' }, statusColumn('entity_type', 'Entity type'), { key: 'entity_id', label: 'Entity ID' }, { key: 'symbol_id', label: 'Symbol' }, { key: 'severity', label: 'Severity', render: (value) => <SeverityBadge value={value} /> }, statusColumn('status', 'Status'), { key: 'message', label: 'Message' }, { key: 'policy_version', label: 'Policy' }], 'conflict_id'],
    corrections: ['/api/v1/governance/stage-corrections', [{ key: 'governance_event_id', label: 'Event ID' }, { key: 'observation_id', label: 'Observation' }, statusColumn('authority', 'Authority'), { key: 'superseded_observation_id', label: 'Superseded' }, { key: 'replacement_observation_id', label: 'Replacement' }, { key: 'available_at', label: 'Available at' }, { key: 'policy_version', label: 'Policy' }], 'governance_event_id'],
    impacts: ['/api/v1/governance/correction-impacts', [{ key: 'impact_id', label: 'Impact ID' }, { key: 'candidate_id', label: 'Candidate' }, statusColumn('impact_link_status', 'Link status'), boolColumn('review_required', 'Review required'), boolColumn('authoritative_calibration_eligible', 'Calibration eligible')], 'impact_id'],
    membership: ['/api/v1/governance/membership-history', [{ key: 'membership_observation_id', label: 'Observation ID' }, { key: 'symbol_id', label: 'Symbol' }, { key: 'sector_id', label: 'Sector' }, statusColumn('membership_trust', 'Trust'), { key: 'valid_from', label: 'Valid from' }, { key: 'valid_to', label: 'Valid to' }], 'membership_observation_id'],
    routing: ['/api/v1/routing/conflicts', [{ key: 'conflict_code', label: 'Conflict' }, { key: 'symbol_id', label: 'Symbol' }, { key: 'requested_tier', label: 'Requested tier' }, { key: 'effective_tier', label: 'Effective tier' }, { key: 'reason', label: 'Reason' }, { key: 'validation_message', label: 'Message' }, { key: 'policy_version', label: 'Policy' }], 'conflict_id'],
  };
  const [basePath, columns, rowId] = configs[tab] ?? configs.conflicts;
  const path = tab === 'impacts' ? withParams(basePath, params, ['status', 'review_required', 'calibration_eligible', 'entity_type']) : basePath;
  const query = usePhase4Query<JsonRecord[]>(path);
  return <div><PageHeader eyebrow="Governance" title="Conflicts and correction authority" description="Conflicts remain explicit. This view never chooses an authoritative winner on the API's behalf." meta={query.data?.meta} /><Tabs options={['conflicts', 'corrections', 'impacts', 'membership', 'routing']} />{tab === 'impacts' ? <FilterBar title="Filter correction impacts" filters={[{ key: 'status', label: 'Link status', kind: 'select', options: options('linked', 'unresolved_legacy_no_match', 'unresolved_legacy_ambiguous') }, { key: 'review_required', label: 'Review required', kind: 'select', options: [{ value: 'true', label: 'Yes' }, { value: 'false', label: 'No' }] }, { key: 'calibration_eligible', label: 'Calibration eligible', kind: 'select', options: [{ value: 'true', label: 'Yes' }, { value: 'false', label: 'No' }] }]} /> : null}<QueryState query={query}>{(response) => <>{tab === 'conflicts' && asRecords(response.data).length ? <ConflictBanner message={`${asRecords(response.data).length} governed conflicts require attention.`} /> : null}<DataTable rows={asRecords(response.data)} columns={columns} rowKey={(row, index) => text(row[rowId], `${tab}-${index}`)} caption="Governance evidence" clientPageSize={tab === 'membership' ? 25 : 0} /></>}</QueryState></div>;
}

export function CalibrationPage() {
  const [params] = useSearchParams();
  const tab = params.get('tab') ?? 'summary';
  const paths: Record<string, string> = { summary: '/api/v1/calibration/summary', manifest: '/api/v1/calibration/manifest', coverage: '/api/v1/calibration/coverage', exclusions: '/api/v1/calibration/exclusions' };
  const path = tab === 'coverage' ? withParams(paths.coverage, params, ['dimension', 'bucket', 'status']) : paths[tab] ?? paths.summary;
  const query = usePhase4Query<unknown>(path);
  return <div><PageHeader eyebrow="Calibration quality" title="Eligibility, coverage, and exclusions" description="Sample counts are shown exactly as provided; the dashboard makes no statistical-confidence claim." meta={query.data?.meta} /><Tabs options={['summary', 'manifest', 'coverage', 'exclusions']} />{tab === 'coverage' ? <FilterBar title="Filter calibration coverage" filters={[{ key: 'dimension', label: 'Dimension', kind: 'text', placeholder: 'market_regime' }, { key: 'bucket', label: 'Bucket', kind: 'text' }, { key: 'status', label: 'Status', kind: 'select', options: options('PASS', 'WARN', 'FAIL', 'NOT_EVALUATED') }]} /> : null}<QueryState query={query}>{(response) => { const rows = asRecords(response.data); if (rows.length) return <DataTable rows={rows} columns={Object.keys(rows[0]).slice(0, 10).map((key) => ({ key, label: key.replace(/_/g, ' ') }))} rowKey={(row, index) => text(row.sample_id ?? row.dimension ?? row.exclusion_reason, String(index))} caption={`Calibration ${tab}`} />; const record = asRecord(response.data); return <KeyValuePanel title={`Calibration ${tab}`} record={record} />; }}</QueryState></div>;
}

export function PerformancePage() {
  const [params] = useSearchParams();
  const tab = params.get('tab') ?? 'latest';
  const paths: Record<string, string> = { latest: '/api/v1/performance/latest', runs: '/api/v1/performance/runs', baselines: '/api/v1/performance/baselines' };
  const path = tab === 'runs' ? withParams(paths.runs, params, ['cache_mode', 'replay_mode', 'performance_status', 'date_from', 'date_to']) : paths[tab] ?? paths.latest;
  const query = usePhase4Query<unknown>(path);
  return <div><PageHeader eyebrow="Operational performance" title="Benchmarks, replay, and baselines" description="Fixture evidence is never presented as a copied-realistic production baseline." meta={query.data?.meta} /><Tabs options={['latest', 'runs', 'baselines']} />{tab === 'runs' ? <FilterBar title="Filter performance runs" filters={[{ key: 'cache_mode', label: 'Cache mode', kind: 'select', options: options('COLD', 'WARM') }, { key: 'replay_mode', label: 'Replay mode', kind: 'select', options: options('REPLAY', 'STANDARD') }, { key: 'performance_status', label: 'Performance status', kind: 'select', options: options('PASS', 'WARN', 'FAIL', 'NOT_EVALUATED') }, { key: 'date_from', label: 'From', kind: 'date' }, { key: 'date_to', label: 'To', kind: 'date' }]} /> : null}{tab === 'baselines' ? <section className="notice notice-warning"><strong>Copied-realistic performance baseline not established.</strong><span>Fixture runs are not production baselines.</span></section> : null}<QueryState query={query}>{(response) => { const rows = asRecords(response.data); return rows.length ? <DataTable rows={rows} columns={[{ key: 'run_id', label: 'Run' }, statusColumn('functional_status', 'Functional status'), statusColumn('performance_status', 'Performance status'), { key: 'total_runtime_ms', label: 'Runtime ms' }, { key: 'peak_rss_mb', label: 'Peak RSS MB' }, { key: 'database_time_ms', label: 'Database ms' }, statusColumn('replay_equivalence', 'Replay'), statusColumn('cache_mode', 'Profile')]} rowKey={(row, index) => text(row.run_id, String(index))} caption="Performance runs" /> : <KeyValuePanel title="Latest performance evidence" record={asRecord(response.data)} />; }}</QueryState></div>;
}

export function ReadinessPage() {
  const [params] = useSearchParams();
  const readiness = usePhase4Query<JsonRecord>('/api/v1/system/readiness');
  const checks = usePhase4Query<JsonRecord[]>(withParams('/api/v1/readiness/checks', params, ['readiness_status']));
  const limitations = usePhase4Query<JsonRecord[]>('/api/v1/system/limitations');
  return <div><PageHeader eyebrow="System readiness" title="Development and production gates" description="Development readiness and production readiness are deliberately separate. Production remains blocked." meta={readiness.data?.meta ?? checks.data?.meta} />
    <QueryState query={readiness}>{(response) => { const data = asRecord(response.data); return <><section className="summary-grid"><Metric label="Formal verdict" value={data.readiness_status} /><article className="metric"><span>Phase 4 development</span><ReadinessBadge value={data.phase4_development_ready ? 'Ready' : 'Not ready'} /></article><article className="metric"><span>Phase 4 production</span><ReadinessBadge value={data.phase4_production_ready ? 'Ready' : 'Not ready'} /></article></section><LimitationList limitations={asRecords(data.limitations).map((item) => text(item.limitation_id, '')).filter(Boolean)} /></>; }}</QueryState>
    <section className="section-card"><h2>Every readiness check</h2><FilterBar title="Filter readiness checks" filters={[{ key: 'readiness_status', label: 'Status', kind: 'select', options: options('PASS', 'WARN', 'FAIL', 'NOT_EVALUATED') }]} /><QueryState query={checks}>{(response) => <DataTable rows={asRecords(response.data)} columns={[{ key: 'check_id', label: 'Check ID' }, { key: 'category', label: 'Category' }, { key: 'severity', label: 'Severity', render: (value) => <SeverityBadge value={value} /> }, statusColumn('status', 'Status'), { key: 'observed_value', label: 'Observed' }, { key: 'expected_condition', label: 'Expected' }, boolColumn('development_blocking', 'Development blocking'), boolColumn('production_blocking', 'Production blocking'), { key: 'limitation', label: 'Limitation' }, { key: 'remediation', label: 'Remediation' }]} rowKey={(row, index) => text(row.check_id, String(index))} caption="Readiness checks" />}</QueryState></section>
    <section className="section-card"><h2>Limitation details</h2><QueryState query={limitations}>{(response) => <DataTable rows={asRecords(response.data)} columns={[{ key: 'limitation_id', label: 'Limitation' }, { key: 'description', label: 'Description' }, { key: 'severity', label: 'Severity', render: (value) => <SeverityBadge value={value} /> }, boolColumn('development_blocking', 'Development blocking'), boolColumn('production_blocking', 'Production blocking'), { key: 'remediation', label: 'Remediation' }]} rowKey={(row, index) => text(row.limitation_id, String(index))} caption="System limitations" />}</QueryState></section>
  </div>;
}

export function DetailPage({ kind }: { kind: 'routing' | 'candidates' | 'positions' | 'alerts' | 'incidents' }) {
  const params = useParams();
  const id = params.id ?? '';
  const base: Record<typeof kind, string> = { routing: '/api/v1/routing', candidates: '/api/v1/candidates', positions: '/api/v1/positions/coverage', alerts: '/api/v1/alerts', incidents: '/api/v1/alert-incidents' };
  const detail = usePhase4Query<JsonRecord>(`${base[kind]}/${encodeURIComponent(id)}`);
  const snapshots = usePhase4Query<JsonRecord[]>(`/api/v1/candidates/${encodeURIComponent(id)}/snapshots`, { enabled: kind === 'candidates' });
  const decisions = usePhase4Query<JsonRecord[]>(`/api/v1/candidates/${encodeURIComponent(id)}/decisions`, { enabled: kind === 'candidates' });
  const outcomes = usePhase4Query<JsonRecord[]>(`/api/v1/candidates/${encodeURIComponent(id)}/outcomes`, { enabled: kind === 'candidates' });
  return <div><PageHeader eyebrow="Read-only detail" title={`${kind.replace(/-/g, ' ')} evidence`} description={`Authoritative API detail for ${id}.`} meta={detail.data?.meta} /><QueryState query={detail}>{(response) => { const record = asRecord(response.data); return <>{record.recovered_from_position_state === true ? <section className="notice notice-warning"><strong>Recovered from live position state</strong><span>Pre-entry history unavailable</span></section> : null}{record.positive_action_suppressed === true ? <section className="notice notice-conflict"><strong>Positive action suppressed</strong><span>{text(record.suppression_reasons)}</span></section> : null}<KeyValuePanel title="Summary" record={record} /></>; }}</QueryState>{kind === 'candidates' ? <section className="detail-columns"><ChildPanel title="Snapshots" query={snapshots} /><ChildPanel title="Decisions" query={decisions} /><ChildPanel title="Outcomes" query={outcomes} missing="No outcome attribution is available; this is not treated as zero or failed performance." /></section> : null}</div>;
}

function ChildPanel({ title, query, missing }: { title: string; query: { isLoading: boolean; error: unknown; data?: ApiEnvelope<JsonRecord[]> }; missing?: string }) {
  return <section className="section-card"><h2>{title}</h2><QueryState query={query}>{(response) => { const rows = asRecords(response.data); if (!rows.length) return <EmptyState detail={missing} />; return <div className="timeline">{rows.map((row, index) => <KeyValuePanel key={text(row.snapshot_id ?? row.decision_context_id ?? row.attribution_id, String(index))} title={`${title} ${index + 1}`} record={row} />)}</div>; }}</QueryState></section>;
}
