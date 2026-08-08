import { useCallback, useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react';
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer,
  Tooltip, XAxis, YAxis,
} from 'recharts';
import { DataTable, EmptyState, ErrorState, PageHeader, StatusBadge } from '../phase4/components';
import { useAuth } from '../phase4/auth';
import type { JsonRecord } from '../phase4/types';
import { journalApi } from './api';
import { JournalChart, type FillMarker, type JournalBar } from './JournalChart';

interface Items<T> { items: T[]; scope?: string; pagination?: JsonRecord }
interface ExposureResponse { scope: string; status?: string; trust_status?: string; as_of_date?: string; metrics: JsonRecord; policy_breaches?: JsonRecord[] }
interface ChartResponse { bars: JournalBar[]; markers: FillMarker[]; trust_status: string; source_snapshot?: JsonRecord }

function useJournal<T>(path: string | null) {
  const { credential } = useAuth();
  const [data, setData] = useState<T>();
  const [error, setError] = useState<unknown>();
  const [loading, setLoading] = useState(Boolean(path));
  const reload = useCallback(() => {
    if (!path) { setData(undefined); setLoading(false); return; }
    setLoading(true); setError(undefined);
    journalApi.get<T>(path, credential).then(setData).catch(setError).finally(() => setLoading(false));
  }, [credential, path]);
  useEffect(reload, [reload]);
  return { data, error, loading, reload };
}

function AccountSelector({ value, onChange }: { value: string; onChange: (value: string) => void }) {
  const accounts = useJournal<Items<string>>('/api/trade-journal/accounts');
  useEffect(() => {
    if (!value && accounts.data?.items[0]) onChange(accounts.data.items[0]);
  }, [accounts.data, onChange, value]);
  return <label className="journal-selector">Account<select value={value} onChange={(event) => onChange(event.target.value)}><option value="">Select account</option>{accounts.data?.items.map((account) => <option key={account}>{account}</option>)}</select></label>;
}

function CheckpointSelector({ account, value, onChange }: { account: string; value: string; onChange: (value: string) => void }) {
  const dates = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/as-of-dates?account_ref=${encodeURIComponent(account)}` : null);
  return <label className="journal-selector">Broker checkpoint<select value={value} onChange={(event) => onChange(event.target.value)}><option value="">Latest available</option>{dates.data?.items.map((item) => <option key={String(item.snapshot_id)} value={String(item.as_of_date)}>{String(item.as_of_date)} · {String(item.market_state)} · {String(item.trust_status)}</option>)}</select></label>;
}

export function TrustBanner({ status, generatedAt }: { status?: unknown; generatedAt?: unknown }) {
  const normalized = String(status ?? 'UNAVAILABLE').toUpperCase();
  const blocked = ['BLOCKED', 'CONFLICT', 'UNTRUSTED', 'UNAVAILABLE'].includes(normalized);
  const partial = ['PARTIAL', 'PROVISIONAL', 'STALE'].includes(normalized);
  return <section className={`notice ${blocked ? 'notice-conflict' : partial ? 'notice-warning' : 'notice-trusted'}`} role={blocked ? 'alert' : 'status'}>
    <strong><StatusBadge value={normalized} /> Actual portfolio evidence</strong>
    <span>{blocked ? 'Some evidence is unavailable or blocked; no missing values are fabricated.' : partial ? 'Use displayed values with the stated coverage limitations.' : 'The displayed components are backed by trusted journal and market evidence.'}{generatedAt ? ` Generated ${String(generatedAt)}.` : ''}</span>
  </section>;
}

function Metric({ label, value, suffix = '' }: { label: string; value: unknown; suffix?: string }) {
  return <div className={`metric ${value == null ? 'metric-unavailable' : ''}`}><span>{label}</span><strong>{value == null ? 'Unavailable' : `${String(value)}${suffix}`}</strong></div>;
}

export function money(value: unknown): string {
  if (value == null || value === '') return 'Unavailable';
  const number = Number(value);
  return Number.isFinite(number) ? number.toLocaleString('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 2 }) : String(value);
}

export function pct(value: unknown): string {
  if (value == null || value === '') return 'Unavailable';
  const number = Number(value);
  return Number.isFinite(number) ? `${(number * 100).toFixed(2)}%` : String(value);
}

function TaskButton({ account, action, label, onComplete }: { account: string; action: 'reconstructions' | 'reconciliations' | 'analyses'; label: string; onComplete?: () => void }) {
  const { credential } = useAuth();
  const [task, setTask] = useState<JsonRecord>();
  const [error, setError] = useState('');
  const launch = async () => {
    const form = new FormData(); form.set('account_ref', account);
    setError('');
    try { setTask(await journalApi.form<JsonRecord>(`/api/trade-journal/${action}`, credential, form)); }
    catch (reason) { setError(reason instanceof Error ? reason.message : 'Task launch failed'); }
  };
  useEffect(() => {
    const runId = task?.journal_run_id;
    if (!runId || ['COMPLETED', 'FAILED'].includes(String(task.status))) return;
    const timer = window.setInterval(() => {
      journalApi.get<JsonRecord>(`/api/trade-journal/tasks/${encodeURIComponent(String(runId))}`, credential)
        .then((next) => { setTask(next); if (next.status === 'COMPLETED') onComplete?.(); })
        .catch((reason) => setError(reason instanceof Error ? reason.message : 'Task status failed'));
    }, 1500);
    return () => window.clearInterval(timer);
  }, [credential, onComplete, task]);
  return <div className="task-action"><button type="button" disabled={!account || Boolean(task && !['COMPLETED', 'FAILED'].includes(String(task.status)))} onClick={launch}>{label}</button>{task ? <StatusBadge value={task.status} /> : null}{error ? <span role="alert">{error}</span> : null}</div>;
}

function GovernancePanel({ account }: { account: string }) {
  const { credential } = useAuth();
  const requests = useJournal<{ adjustments: JsonRecord[]; corporate_actions: JsonRecord[] }>(account ? `/api/trade-journal/governance/requests?account_ref=${encodeURIComponent(account)}` : null);
  const [message, setMessage] = useState('');
  const [reviewer, setReviewer] = useState('local-operator');
  const submitOpening = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault(); const form = new FormData(event.currentTarget); form.set('account_ref', account);
    try { await journalApi.form('/api/trade-journal/opening-lots/propose', credential, form); setMessage('Opening-lot proposal retained for review.'); requests.reload(); event.currentTarget.reset(); }
    catch (reason) { setMessage(reason instanceof Error ? reason.message : 'Proposal failed'); }
  };
  const submitAdjustment = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault(); const form = new FormData(event.currentTarget); form.set('account_ref', account); form.set('adjustment_type', 'manual_adjustment');
    try { await journalApi.form('/api/trade-journal/adjustments/propose', credential, form); setMessage('Manual-adjustment proposal retained for review.'); requests.reload(); event.currentTarget.reset(); }
    catch (reason) { setMessage(reason instanceof Error ? reason.message : 'Proposal failed'); }
  };
  const submitCorporateAction = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault(); const form = new FormData(event.currentTarget);
    try { await journalApi.form('/api/trade-journal/corporate-actions/propose', credential, form); setMessage('Corporate-action proposal retained for review.'); requests.reload(); event.currentTarget.reset(); }
    catch (reason) { setMessage(reason instanceof Error ? reason.message : 'Proposal failed'); }
  };
  const approve = async (id: string, kind: 'opening-lots' | 'adjustments' | 'corporate-actions') => {
    const form = new FormData(); form.set('reviewer', reviewer);
    try { await journalApi.form(`/api/trade-journal/${kind}/${encodeURIComponent(id)}/approve`, credential, form); setMessage('Approval recorded; rerun reconstruction and analysis.'); requests.reload(); }
    catch (reason) { setMessage(reason instanceof Error ? reason.message : 'Approval failed'); }
  };
  return <section className="section-card"><h2>Reviewed inventory and adjustments</h2><p className="muted">Proposals never alter the ledger until explicitly approved. Opening lots must predate the reconstruction start.</p>
    <form className="journal-form" onSubmit={submitOpening}><label>Instrument ID<input name="instrument_id" required /></label><label>Effective timestamp<input name="effective_at" type="datetime-local" required /></label><label>Quantity<input name="quantity" inputMode="decimal" required /></label><label>Total cost<input name="total_cost" inputMode="decimal" required /></label><label>Reason<input name="reason" required /></label><button type="submit">Propose opening lot</button></form>
    <details><summary>Propose another reviewed event</summary><h3>Manual adjustment</h3><form className="journal-form" onSubmit={submitAdjustment}><label>Instrument ID<input name="instrument_id" required /></label><label>Effective timestamp<input name="effective_at" type="datetime-local" required /></label><label>Quantity change<input name="quantity" inputMode="decimal" /></label><label>Amount<input name="amount" inputMode="decimal" /></label><label>Reason<input name="reason" required /></label><button type="submit">Propose adjustment</button></form><h3>Split or bonus</h3><form className="journal-form" onSubmit={submitCorporateAction}><label>Instrument ID<input name="instrument_id" required /></label><label>Action<select name="action_type"><option value="split">Split</option><option value="bonus">Bonus</option></select></label><label>Effective date<input name="effective_date" type="date" required /></label><label>Quantity factor<input name="quantity_factor" inputMode="decimal" required /></label><label>Cost factor<input name="cost_factor" inputMode="decimal" /></label><label>Source reference<input name="source_ref" required /></label><button type="submit">Propose corporate action</button></form></details>
    <label className="journal-selector">Reviewer<input value={reviewer} onChange={(event) => setReviewer(event.target.value)} /></label>{message ? <p role="status">{message}</p> : null}
    <DataTable rows={requests.data?.adjustments ?? []} caption="Pending manual review requests" rowKey={(row) => String(row.adjustment_id)} columns={[{ key: 'adjustment_type', label: 'Type' }, { key: 'instrument_id', label: 'Instrument' }, { key: 'effective_at', label: 'Effective' }, { key: 'quantity', label: 'Quantity' }, { key: 'status', label: 'Status', render: (value) => <StatusBadge value={value} /> }, { key: 'approve', label: 'Review', render: (_, row) => row.status === 'PROPOSED' ? <button type="button" onClick={() => approve(String(row.adjustment_id), row.adjustment_type === 'opening_lot' ? 'opening-lots' : 'adjustments')}>Approve</button> : null }]} />
    <DataTable rows={requests.data?.corporate_actions ?? []} caption="Corporate action review requests" rowKey={(row) => String(row.action_id)} columns={[{ key: 'action_type', label: 'Action' }, { key: 'instrument_id', label: 'Instrument' }, { key: 'effective_date', label: 'Effective' }, { key: 'quantity_factor', label: 'Quantity factor' }, { key: 'review_status', label: 'Status', render: (value) => <StatusBadge value={value} /> }, { key: 'approve', label: 'Review', render: (_, row) => row.review_status === 'PROPOSED' ? <button type="button" onClick={() => approve(String(row.action_id), 'corporate-actions')}>Approve</button> : null }]} />
  </section>;
}

export function ImportReconciliationPage() {
  const { credential } = useAuth();
  const [kind, setKind] = useState<'tradebook' | 'holdings'>('tradebook');
  const [file, setFile] = useState<File>(); const [account, setAccount] = useState(''); const [asOf, setAsOf] = useState('');
  const [preview, setPreview] = useState<JsonRecord>(); const [message, setMessage] = useState(''); const [busy, setBusy] = useState(false);
  const [selectedReconciliation, setSelectedReconciliation] = useState('');
  const [checkpointDate, setCheckpointDate] = useState('');
  const imports = useJournal<Items<JsonRecord>>('/api/trade-journal/imports?limit=50');
  const issues = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/dq-issues?account_ref=${encodeURIComponent(account)}&limit=50` : null);
  const reconciliations = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/reconciliations?account_ref=${encodeURIComponent(account)}` : null);
  const evidence = useJournal<{ reconciliation: JsonRecord; items: JsonRecord[] }>(selectedReconciliation ? `/api/trade-journal/reconciliations/${encodeURIComponent(selectedReconciliation)}` : null);
  useEffect(() => {
    if (!checkpointDate) return;
    const match = reconciliations.data?.items.find((row) => String(row.as_of_at).slice(0, 10) === checkpointDate);
    if (match) setSelectedReconciliation(String(match.reconciliation_id));
  }, [checkpointDate, reconciliations.data]);
  const submit = async (event: FormEvent, commit: boolean) => {
    event.preventDefault(); if (!file) return; setBusy(true); setMessage(''); const form = new FormData(); form.set('file', file);
    if (commit) { form.set('broker', 'dhan'); form.set('account_ref', account); form.set('expected_sha256', String(preview?.file_sha256 ?? '')); if (kind === 'holdings') { form.set('as_of', asOf); form.set('market_state', 'eod'); form.set('mode', 'reconciliation_only'); } }
    try { const result = await journalApi.form<JsonRecord>(`/api/trade-journal/imports/${kind}/${commit ? 'commit' : 'preview'}`, credential, form); if (commit) { setMessage(`Import ${String(result.status).toLowerCase()}.`); imports.reload(); issues.reload(); } else setPreview(result); }
    catch (reason) { setMessage(reason instanceof Error ? reason.message : 'Import request failed'); } finally { setBusy(false); }
  };
  return <><PageHeader eyebrow="Actual portfolio" title="Import & Reconciliation" description="Preview broker files, bind commits to their SHA-256, and inspect retained reconciliation evidence." /><AccountSelector value={account} onChange={(value) => { setAccount(value); setCheckpointDate(''); setSelectedReconciliation(''); }} /><CheckpointSelector account={account} value={checkpointDate} onChange={setCheckpointDate} />
    <section className="notice notice-warning"><strong>Local operator workflow</strong><span>Preview is pure. Holdings default to reconciliation-only and never fabricate opening inventory.</span></section>
    <section className="section-card"><h2>Broker file</h2><form className="journal-form" onSubmit={(event) => submit(event, false)}><label>File type<select value={kind} onChange={(event) => { setKind(event.target.value as typeof kind); setPreview(undefined); }}><option value="tradebook">Dhan tradebook</option><option value="holdings">Dhan holdings</option></select></label><label>Account<input value={account} onChange={(event) => setAccount(event.target.value)} required /></label>{kind === 'holdings' ? <label>As-of date<input type="date" value={asOf} onChange={(event) => setAsOf(event.target.value)} required /></label> : null}<label>File<input type="file" accept={kind === 'tradebook' ? '.xlsx' : '.csv'} onChange={(event) => { setFile(event.target.files?.[0]); setPreview(undefined); }} required /></label><button type="submit" disabled={busy}>Preview</button>{preview ? <button type="button" className="primary" disabled={busy || !account} onClick={(event) => submit(event as unknown as FormEvent, true)}>Commit exact previewed file</button> : null}</form>{message ? <p role="status">{message}</p> : null}{preview ? <dl className="detail-grid"><dt>SHA-256</dt><dd><code>{String(preview.file_sha256)}</code></dd><dt>Rows</dt><dd>{String(preview.rows)}</dd><dt>Detected format</dt><dd>{String(preview.format_version)}</dd></dl> : <EmptyState title="No preview yet" detail="Choose a supported broker export to validate it without writing journal state." />}</section>
    <section className="task-strip"><TaskButton account={account} action="reconstructions" label="Reconstruct ledger" onComplete={reconciliations.reload} /><TaskButton account={account} action="reconciliations" label="Reconcile checkpoint" onComplete={reconciliations.reload} /></section>
    <DataTable rows={imports.data?.items ?? []} caption="Import history" rowKey={(row) => String(row.import_id)} columns={[{ key: 'created_at', label: 'Created' }, { key: 'file_type', label: 'Type' }, { key: 'account_ref', label: 'Account' }, { key: 'status', label: 'Status', render: (value) => <StatusBadge value={value} /> }, { key: 'row_count', label: 'Rows' }]} />
    {account ? <><section className="section-card"><h2>Data-quality evidence</h2>{issues.error ? <ErrorState error={issues.error} /> : <DataTable rows={issues.data?.items ?? []} caption="Journal DQ issues" rowKey={(row) => String(row.issue_id)} columns={[{ key: 'severity', label: 'Severity', render: (value) => <StatusBadge value={value} kind="severity" /> }, { key: 'issue_type', label: 'Issue' }, { key: 'entity_id', label: 'Entity' }, { key: 'lifecycle_status', label: 'Status' }]} />}</section>
      <section className="section-card"><h2>Reconciliation history</h2><DataTable rows={reconciliations.data?.items ?? []} caption="Reconciliation runs" rowKey={(row) => String(row.reconciliation_id)} onRow={(row) => setSelectedReconciliation(String(row.reconciliation_id))} columns={[{ key: 'as_of_at', label: 'As of' }, { key: 'status', label: 'Status', render: (value) => <StatusBadge value={value} /> }, { key: 'matched_count', label: 'Matched' }, { key: 'issue_count', label: 'Issues' }, { key: 'trust_status', label: 'Trust', render: (value) => <StatusBadge value={value} /> }]} />{evidence.data ? <DataTable rows={evidence.data.items} caption="Selected reconciliation evidence" rowKey={(row) => String(row.instrument)} columns={[{ key: 'instrument', label: 'Instrument' }, { key: 'classification', label: 'Classification' }, { key: 'broker_quantity', label: 'Broker qty' }, { key: 'ledger_quantity', label: 'Ledger qty' }, { key: 'broker_cost', label: 'Broker cost' }, { key: 'fifo_cost', label: 'FIFO cost' }, { key: 'weighted_average_cost', label: 'Weighted-average cost' }]} /> : null}</section><GovernancePanel account={account} /></> : null}</>;
}

function ExposureChart({ title, values }: { title: string; values: unknown }) {
  const data = Object.entries((values ?? {}) as Record<string, unknown>).map(([name, value]) => ({ name, weight: Number(value) * 100 })).filter((row) => Number.isFinite(row.weight));
  return <section className="section-card"><h2>{title}</h2>{data.length ? <ResponsiveContainer width="100%" height={260}><BarChart data={data} layout="vertical"><CartesianGrid stroke="#29404f" /><XAxis type="number" unit="%" /><YAxis dataKey="name" type="category" width={100} /><Tooltip formatter={(value) => `${Number(value).toFixed(2)}%`} /><Bar dataKey="weight" fill="#65d6c3" /></BarChart></ResponsiveContainer> : <EmptyState />}</section>;
}

export function ActualPortfolioPage() {
  const [account, setAccount] = useState('');
  const positions = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/positions?account_ref=${encodeURIComponent(account)}` : null);
  const exposure = useJournal<ExposureResponse>(account ? `/api/trade-journal/exposures?account_ref=${encodeURIComponent(account)}` : null);
  const series = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/portfolio-series?account_ref=${encodeURIComponent(account)}&limit=250` : null);
  const reload = useCallback(() => { positions.reload(); exposure.reload(); series.reload(); }, [exposure.reload, positions.reload, series.reload]);
  const metrics = exposure.data?.metrics ?? {}; const exposures = (metrics.exposures ?? {}) as JsonRecord;
  return <><PageHeader eyebrow="Broker-derived" title="Actual Portfolio" description="Reconstructed cash-equity positions. This view is visibly separate from system-generated positions." /><AccountSelector value={account} onChange={setAccount} />
    <section className="notice notice-warning"><strong>Scope: securities only / holdings only</strong><span>Cash, charges, taxes, NAV, TWR, XIRR and total-account returns are not inferred.</span></section>
    <section className="task-strip"><TaskButton account={account} action="reconstructions" label="Reconstruct positions" onComplete={reload} /><TaskButton account={account} action="analyses" label="Run point-in-time analysis" onComplete={reload} /></section>
    {exposure.loading ? <div className="loading">Loading portfolio analytics…</div> : exposure.error ? <ErrorState error={exposure.error} /> : <><TrustBanner status={exposure.data?.trust_status ?? exposure.data?.status} /><section className="summary-grid"><Metric label="Market value" value={money(metrics.market_value)} /><Metric label="Unrealised P&L" value={money(metrics.unrealised_pnl)} /><Metric label="Realised gross FIFO P&L" value={money(metrics.realised_gross_fifo_pnl)} /><Metric label="Max holdings-only drawdown" value={pct(metrics.max_drawdown)} /><Metric label="Top position weight" value={pct(metrics.top_1_weight)} /><Metric label="Top five weight" value={pct(metrics.top_5_weight)} /><Metric label="Stop coverage" value={pct(metrics.stop_coverage)} /><Metric label="Known-stop heat" value={pct(metrics.known_stop_heat)} /></section></>}
    <section className="section-card"><h2>Holdings-only value and drawdown</h2>{series.data?.items.length ? <ResponsiveContainer width="100%" height={320}><AreaChart data={series.data.items}><CartesianGrid stroke="#29404f" /><XAxis dataKey="as_of_date" /><YAxis yAxisId="value" tickFormatter={(value) => Number(value).toLocaleString('en-IN')} /><YAxis yAxisId="drawdown" orientation="right" tickFormatter={(value) => `${(Number(value) * 100).toFixed(0)}%`} /><Tooltip /><Legend /><Area yAxisId="value" dataKey="market_value" name="Market value" stroke="#65d6c3" fill="#65d6c344" /><Area yAxisId="drawdown" dataKey="drawdown" name="Drawdown" stroke="#ff8d8d" fill="#ff8d8d22" /></AreaChart></ResponsiveContainer> : <EmptyState title="Analysis not available" detail="Run reconstruction and point-in-time analysis to build the holdings-only series." />}</section>
    {positions.loading ? <div className="loading">Loading actual positions…</div> : positions.error ? <ErrorState error={positions.error} /> : <DataTable rows={positions.data?.items ?? []} caption="Actual reconstructed positions" rowKey={(row) => String(row.instrument_id)} columns={[{ key: 'symbol', label: 'Symbol' }, { key: 'instrument_id', label: 'Instrument' }, { key: 'quantity', label: 'Quantity' }, { key: 'fifo_cost', label: 'FIFO cost' }, { key: 'weighted_average_cost', label: 'Weighted-average cost' }, { key: 'trust_status', label: 'Trust', render: (value) => <StatusBadge value={value} /> }]} />}
    <section className="section-card"><h2>Concentration policy evidence</h2><DataTable rows={exposure.data?.policy_breaches ?? []} caption="Portfolio policy breaches" rowKey={(row) => String(row.breach_id)} columns={[{ key: 'rule_code', label: 'Policy' }, { key: 'observed_value', label: 'Observed' }, { key: 'threshold_value', label: 'Threshold' }, { key: 'policy_version', label: 'Version' }]} /></section>
    <div className="detail-columns"><ExposureChart title="Sector exposure" values={exposures.sector} /><ExposureChart title="Stage exposure" values={exposures.stage} /><ExposureChart title="Pattern exposure" values={exposures.pattern} /></div></>;
}

function AnnotationForm({ episodeId, onSaved }: { episodeId: string; onSaved: () => void }) {
  const { credential } = useAuth(); const [message, setMessage] = useState('');
  const submit = async (event: FormEvent<HTMLFormElement>) => { event.preventDefault(); const form = new FormData(event.currentTarget); form.set('episode_id', episodeId); try { await journalApi.form('/api/trade-journal/annotations', credential, form); setMessage('Append-only annotation saved.'); onSaved(); } catch (reason) { setMessage(reason instanceof Error ? reason.message : 'Annotation failed'); } };
  return <form className="journal-form" onSubmit={submit}><label>Thesis<textarea name="thesis" /></label><label>Setup<input name="setup" /></label><label>Intended stop<input name="intended_stop" inputMode="decimal" /></label><label>Target<input name="target" inputMode="decimal" /></label><label>Exit reason<input name="exit_reason" /></label><label>Lesson<textarea name="lesson" /></label><button type="submit">Append annotation</button>{message ? <span role="status">{message}</span> : null}</form>;
}

export function TradingJournalPage() {
  const [account, setAccount] = useState(''); const [selected, setSelected] = useState('');
  const episodes = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/episodes?account_ref=${encodeURIComponent(account)}&limit=250` : null);
  const detail = useJournal<{ episode: JsonRecord; fills: JsonRecord[]; annotations: JsonRecord[] }>(selected ? `/api/trade-journal/episodes/${encodeURIComponent(selected)}` : null);
  const chart = useJournal<ChartResponse>(selected ? `/api/trade-journal/episodes/${encodeURIComponent(selected)}/chart` : null);
  return <><PageHeader eyebrow="Gross process record" title="Trading Journal" description="Zero-to-zero episodes retain additions, trims, aliases and year-boundary carryover without rewriting history." /><AccountSelector value={account} onChange={(value) => { setAccount(value); setSelected(''); }} />
    {episodes.error ? <ErrorState error={episodes.error} /> : <DataTable rows={episodes.data?.items ?? []} caption="Trade episodes" rowKey={(row) => String(row.episode_id)} onRow={(row) => setSelected(String(row.episode_id))} columns={[{ key: 'opened_at', label: 'Opened' }, { key: 'closed_at', label: 'Closed' }, { key: 'instrument_id', label: 'Instrument' }, { key: 'status', label: 'Cycle' }, { key: 'realised_gross_pnl', label: 'Gross FIFO P&L' }, { key: 'trust_status', label: 'Trust', render: (value) => <StatusBadge value={value} /> }]} />}
    {selected ? <section className="section-card"><h2>Episode detail</h2>{detail.loading || chart.loading ? <div className="loading">Loading episode evidence…</div> : detail.error || chart.error ? <ErrorState error={detail.error ?? chart.error} /> : <><TrustBanner status={chart.data?.trust_status} /><JournalChart bars={chart.data?.bars ?? []} markers={chart.data?.markers ?? []} /><DataTable rows={detail.data?.fills ?? []} caption="Episode fills" rowKey={(row) => String(row.fill_id)} columns={[{ key: 'trade_date', label: 'Exchange date' }, { key: 'executed_at', label: 'UTC timestamp' }, { key: 'symbol', label: 'Symbol' }, { key: 'side', label: 'Side' }, { key: 'quantity', label: 'Quantity' }, { key: 'price', label: 'Price' }, { key: 'link_type', label: 'Episode role' }]} /><h3>Append-only journal notes</h3><AnnotationForm episodeId={selected} onSaved={detail.reload} /><DataTable rows={detail.data?.annotations ?? []} caption="Annotation revisions" rowKey={(row) => String(row.annotation_id)} columns={[{ key: 'revision', label: 'Revision' }, { key: 'thesis', label: 'Thesis' }, { key: 'setup', label: 'Setup' }, { key: 'intended_stop', label: 'Stop' }, { key: 'exit_reason', label: 'Exit reason' }, { key: 'lesson', label: 'Lesson' }, { key: 'created_at', label: 'Created' }]} /></>}</section> : <EmptyState title="Select an episode" detail="Choose an episode row to inspect its candlesticks, fill markers, evaluations and annotations." />}</>;
}

function Components({ value }: { value: unknown }): ReactNode {
  const payload = value as JsonRecord | undefined; const components = (payload?.process_components ?? {}) as JsonRecord; const contributions = (payload?.component_contributions ?? {}) as JsonRecord;
  return <details><summary>Components</summary><ul className="inline-list">{Object.entries(components).map(([name, score]) => <li key={name}>{name}: {String(score ?? 'missing')} ({String(contributions[name] ?? 'no contribution')})</li>)}</ul></details>;
}

export function BehaviourPerformancePage() {
  const [account, setAccount] = useState('');
  const evaluations = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/evaluations?account_ref=${encodeURIComponent(account)}` : null);
  const trades = useJournal<Items<JsonRecord>>(account ? `/api/trade-journal/trade-evaluations?account_ref=${encodeURIComponent(account)}&limit=250` : null);
  const behaviour = useJournal<Items<JsonRecord> & { eligible: number }>(account ? `/api/trade-journal/behaviour?account_ref=${encodeURIComponent(account)}` : null);
  const portfolioRows = useMemo(() => (evaluations.data?.items ?? []).map((row) => ({ ...row, ...JSON.parse(String(row.metrics_json ?? '{}')) })), [evaluations.data]);
  return <><PageHeader eyebrow="Versioned analytics" title="Behaviour & Performance" description="Contemporaneous process quality remains separate from ex-post outcomes; findings require eligible minimum cohorts." /><AccountSelector value={account} onChange={setAccount} /><section className="notice notice-warning"><strong>Gross / holdings-only scopes</strong><span>No net-return or total-account performance is shown without funds, charges and tax inputs.</span></section>
    <section className="section-card"><h2>Portfolio evaluations</h2><DataTable rows={portfolioRows} caption="Versioned portfolio evaluations" rowKey={(row) => String(row.evaluation_id)} columns={[{ key: 'as_of_date', label: 'As of' }, { key: 'scope_label', label: 'Scope' }, { key: 'realised_gross_fifo_pnl', label: 'Realised gross FIFO' }, { key: 'profit_factor', label: 'Profit factor' }, { key: 'expectancy_per_closed_episode', label: 'Expectancy' }, { key: 'turnover', label: 'Turnover' }, { key: 'trust_status', label: 'Trust', render: (value) => <StatusBadge value={value} /> }]} /></section>
    <section className="section-card"><h2>Entry, add and exit process evaluations</h2><DataTable rows={trades.data?.items ?? []} caption="Trade process evaluations" rowKey={(row) => String(row.evaluation_id)} columns={[{ key: 'trade_date', label: 'Exchange date' }, { key: 'symbol', label: 'Symbol' }, { key: 'evaluation_type', label: 'Type' }, { key: 'score', label: 'Process score' }, { key: 'score_status', label: 'Coverage', render: (value) => <StatusBadge value={value} /> }, { key: 'classification', label: 'Inferred reason' }, { key: 'components', label: 'Breakdown', render: (value) => <Components value={value} /> }]} /></section>
    <section className="section-card"><h2>Behaviour cohorts</h2>{behaviour.data?.items.length ? <DataTable rows={behaviour.data.items} caption="Behaviour findings" rowKey={(row) => String(row.classification)} columns={[{ key: 'classification', label: 'Finding' }, { key: 'occurrences', label: 'Numerator' }, { key: 'eligible', label: 'Denominator' }, { key: 'prevalence', label: 'Prevalence' }, { key: 'wilson_95', label: '95% Wilson interval' }, { key: 'status', label: 'Status', render: (value) => <StatusBadge value={value} /> }]} /> : <EmptyState title="Insufficient sample or analysis not run" detail="Findings require at least five eligible observations and three occurrences; smaller cohorts remain explicitly insufficient." />}</section></>;
}
