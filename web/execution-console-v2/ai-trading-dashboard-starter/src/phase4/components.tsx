import { useMemo, useState, type ReactNode } from 'react';
import { NavLink, useLocation, useNavigate, useSearchParams } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { useAuth } from './auth';
import type { Freshness, JsonRecord, ResponseMeta } from './types';
import { Phase4ApiError, text } from './types';

const LIMITATION_COPY: Record<string, string> = {
  SINGLE_YEAR_CONCENTRATION: 'Calibration evidence is concentrated in a single year.',
  COPIED_REALISTIC_BASELINE_MISSING: 'Copied-realistic performance baseline not established.',
  OPERATOR_MIGRATIONS_NOT_APPLIED: 'Required operator-store migrations have not been applied.',
  EMPTY_REAL_PHASE3B_HISTORY: 'No real Phase 3B structural history is available.',
  SOURCE_NOT_MIGRATED: 'The optional source schema is not migrated.',
  SOURCE_EMPTY: 'The authoritative source exists but contains no records.',
  SOURCE_UNAVAILABLE: 'The authoritative source is unavailable.',
  LINEAGE_UNAVAILABLE: 'Authoritative lineage is unavailable.',
  FRESHNESS_UNKNOWN: 'Source freshness cannot be established.',
};

export function StatusBadge({ value, kind = 'status' }: { value?: unknown; kind?: string }) {
  const label = text(value, 'Unknown');
  const normalized = label.toLowerCase().replace(/[^a-z0-9]+/g, '-');
  return <span className={`badge badge-${kind} badge-${normalized}`} aria-label={`${kind}: ${label}`}><span aria-hidden="true">●</span> {label}</span>;
}

export const FreshnessBadge = ({ freshness }: { freshness?: Freshness }) => (
  <StatusBadge value={freshness?.freshness_status ?? 'UNKNOWN'} kind="freshness" />
);

export const SeverityBadge = ({ value }: { value?: unknown }) => <StatusBadge value={value} kind="severity" />;
export const ReadinessBadge = ({ value }: { value?: unknown }) => <StatusBadge value={value} kind="readiness" />;

export function LimitationList({ limitations }: { limitations?: string[] }) {
  if (!limitations?.length) return null;
  return <ul className="limitation-list">{limitations.map((code) => <li key={code}><code>{code}</code><span>{LIMITATION_COPY[code] ?? 'The API reported a limitation for this view.'}</span></li>)}</ul>;
}

export function PartialDataBanner({ meta }: { meta?: ResponseMeta }) {
  if (!meta?.partial) return null;
  return <section className="notice notice-warning" role="status"><strong>Partial data</strong><span>Displayed values cover available records only.</span><LimitationList limitations={meta.limitations} /></section>;
}

export function ConflictBanner({ message = 'Governance conflict: no authoritative value can be selected.' }: { message?: string }) {
  return <section className="notice notice-conflict" role="alert"><strong>Conflict</strong><span>{message}</span><NavLink to="/governance">View governance evidence</NavLink></section>;
}

export function LineageSummary({ meta }: { meta?: ResponseMeta }) {
  const primary = meta?.lineage_meta?.primary ?? meta?.lineage?.[0];
  return <details className="lineage"><summary>Lineage and freshness</summary><dl className="detail-grid">
    <dt>Primary source</dt><dd>{primary ? `${primary.source_type} · ${primary.source_id}` : 'Unavailable'}</dd>
    <dt>Run</dt><dd>{text(primary?.run_id)}</dd><dt>Schema</dt><dd>{text(primary?.schema_version)}</dd>
    <dt>Source as-of</dt><dd>{text(meta?.freshness?.source_as_of)}</dd><dt>Latest run</dt><dd>{text(meta?.freshness?.last_successful_run_at)}</dd>
    <dt>Market session</dt><dd>{text(meta?.freshness?.latest_market_session)}</dd><dt>Expected session</dt><dd>{text(meta?.freshness?.expected_market_session)}</dd>
    <dt>Staleness</dt><dd>{meta?.freshness?.staleness_sessions == null ? 'Unknown' : `${meta.freshness.staleness_sessions} sessions`}</dd>
    <dt>Source consistency</dt><dd>{meta?.lineage_meta?.source_consistent === false ? 'Mismatch' : 'Consistent or unknown'}</dd>
  </dl>{meta?.freshness?.freshness_reasons?.length ? <p>{meta.freshness.freshness_reasons.join(' · ')}</p> : null}</details>;
}

export const LineageDrawer = LineageSummary;

export function EmptyState({ title = 'No available records', detail = 'The API returned an empty collection. This is not interpreted as zero activity.' }: { title?: string; detail?: string }) {
  return <div className="state-card" role="status"><strong>{title}</strong><p>{detail}</p></div>;
}
export function UnavailableState({ detail = 'The authoritative source is unavailable.' }: { detail?: string }) { return <div className="state-card unavailable" role="status"><strong>Unavailable</strong><p>{detail}</p></div>; }

export function ErrorState({ error }: { error: unknown }) {
  if (error instanceof Phase4ApiError) {
    const content: Record<number, [string, string]> = {
      401: ['Authentication required', 'The credential is missing or no longer valid. Sign out and authenticate again.'],
      403: ['Authorization denied', 'This credential cannot access the requested operator evidence.'],
      404: ['Resource not found', 'The requested read-only resource does not exist.'],
      409: ['Governance conflict', 'No authoritative value is shown because governed evidence conflicts.'],
      429: ['Rate limited', 'Wait briefly, then reload the displayed data.'],
      503: ['Source unavailable', 'The API cannot currently read the authoritative source.'],
    };
    const [title, detail] = content[error.status] ?? ['Request failed', error.body.message];
    return <div className={`state-card error status-${error.status}`} role="alert"><strong>{title}</strong><p>{detail}</p><code>Request: {error.body.request_id ?? 'unavailable'}</code>{error.status === 409 ? <NavLink to="/governance">Open Governance</NavLink> : null}</div>;
  }
  const timeout = error instanceof DOMException && (error.name === 'TimeoutError' || error.name === 'AbortError');
  return <div className="state-card error" role="alert"><strong>{timeout ? 'Network timeout' : 'Unexpected error'}</strong><p>{timeout ? 'The API did not respond within the bounded timeout.' : 'The dashboard could not load this read-only view.'}</p></div>;
}

export function AsOfSelector({ supported = true }: { supported?: boolean }) {
  const [params, setParams] = useSearchParams();
  const value = params.get('as_of') ?? '';
  const max = new Date().toISOString().slice(0, 10);
  if (!supported) return <p className="muted">This page exposes the latest governed state; historical as-of is not supported by its API endpoints.</p>;
  return <div className="as-of"><label htmlFor="as-of">Evidence as of</label><input id="as-of" type="date" max={max} value={value.slice(0, 10)} onChange={(event) => { const next = new URLSearchParams(params); event.target.value ? next.set('as_of', event.target.value) : next.delete('as_of'); next.delete('cursor'); setParams(next); }} />{value ? <button type="button" onClick={() => { const next = new URLSearchParams(params); next.delete('as_of'); setParams(next); }}>Return to latest</button> : <span>Latest</span>}</div>;
}

export function PaginationControls({ meta, onNext, onPrevious, canPrevious }: { meta?: ResponseMeta; onNext?: () => void; onPrevious?: () => void; canPrevious?: boolean }) {
  if (!meta?.pagination) return null;
  return <nav className="pagination" aria-label="Pagination"><button type="button" disabled={!canPrevious} onClick={onPrevious}>Previous page</button><span>Limit {meta.pagination.limit}</span><button type="button" disabled={!meta.pagination.has_more} onClick={onNext}>Next page</button></nav>;
}

export interface Column { key: string; label: string; render?: (value: unknown, row: JsonRecord) => ReactNode }
export function DataTable({ rows, columns, rowKey, caption, onRow }: { rows: JsonRecord[]; columns: Column[]; rowKey: (row: JsonRecord, index: number) => string; caption: string; onRow?: (row: JsonRecord) => void }) {
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const visible = useMemo(() => columns.filter((column) => !hidden.has(column.key)), [columns, hidden]);
  if (!rows.length) return <EmptyState />;
  return <><details className="column-picker"><summary>Column visibility</summary><div>{columns.map((column) => <label key={column.key}><input type="checkbox" checked={!hidden.has(column.key)} onChange={() => setHidden((current) => { const next = new Set(current); next.has(column.key) ? next.delete(column.key) : next.add(column.key); return next; })} />{column.label}</label>)}</div></details><div className="table-wrap" tabIndex={0}><table><caption className="sr-only">{caption}</caption><thead><tr>{visible.map((column) => <th key={column.key} scope="col">{column.label}</th>)}</tr></thead><tbody>{rows.map((row, index) => <tr key={rowKey(row, index)} tabIndex={onRow ? 0 : undefined} className={onRow ? 'clickable' : ''} onClick={() => onRow?.(row)} onKeyDown={(event) => { if (onRow && (event.key === 'Enter' || event.key === ' ')) { event.preventDefault(); onRow(row); } }}>{visible.map((column) => <td key={column.key}>{column.render ? column.render(row[column.key], row) : text(row[column.key])}</td>)}</tr>)}</tbody></table></div></>;
}

const navItems = [
  ['/', 'Overview'], ['/market', 'Market & Sectors'], ['/routing', 'Routing'], ['/candidates', 'Candidates'], ['/positions', 'Positions'], ['/alerts', 'Alerts'], ['/governance', 'Governance'], ['/calibration', 'Calibration'], ['/performance', 'Performance'], ['/readiness', 'System Readiness'],
] as const;

export function AppShell({ children, readiness }: { children: ReactNode; readiness?: JsonRecord }) {
  const auth = useAuth();
  const location = useLocation();
  const queryClient = useQueryClient();
  const productionReady = readiness?.phase4_production_ready === true;
  const limitations = Array.isArray(readiness?.limitations) ? readiness.limitations : [];
  return <div className="operator-app"><a className="skip-link" href="#main-content">Skip to content</a><header className="topbar"><div><span className="eyebrow">AI Trading System</span><strong>Phase 4B Operator View</strong></div><div className="top-status"><StatusBadge value="API connected" /><FreshnessBadge freshness={{ freshness_status: readiness ? 'FRESH' : 'UNKNOWN' }} /><button type="button" onClick={auth.signOut}>Sign out</button></div></header>{!productionReady ? <div className="production-banner" role="alert"><strong>Development view only — production readiness is blocked.</strong><span>{limitations.length || 4} active limitations</span><NavLink to="/readiness">View readiness</NavLink></div> : null}<div className="app-grid"><nav className="side-nav" aria-label="Primary">{navItems.map(([to, label]) => <NavLink key={to} to={to} end={to === '/'}>{label}</NavLink>)}</nav><main id="main-content"><div className="page-tools"><span>Current view: {navItems.find(([to]) => to === location.pathname)?.[1] ?? 'Detail'}</span><button type="button" onClick={() => queryClient.invalidateQueries()}>Reload displayed data</button></div>{children}</main></div></div>;
}

export function PageHeader({ eyebrow, title, description, meta, asOf = false }: { eyebrow: string; title: string; description: string; meta?: ResponseMeta; asOf?: boolean }) {
  return <><header className="page-header"><div><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p>{description}</p></div><div className="header-status"><FreshnessBadge freshness={meta?.freshness} />{meta?.as_of ? <span>As of {meta.as_of}</span> : null}</div></header><AsOfSelector supported={asOf} /><PartialDataBanner meta={meta} /><LineageSummary meta={meta} /></>;
}

export function KeyValuePanel({ record, title }: { record: JsonRecord; title: string }) {
  const entries = Object.entries(record).filter(([, value]) => value !== null && value !== undefined);
  return <section className="section-card"><h2>{title}</h2>{entries.length ? <dl className="detail-grid">{entries.map(([key, value]) => <div key={key}><dt>{key.replace(/_/g, ' ')}</dt><dd>{text(value)}</dd></div>)}</dl> : <EmptyState />}</section>;
}

export function Tabs({ options }: { options: readonly string[] }) {
  const [params, setParams] = useSearchParams();
  const active = params.get('tab') ?? options[0];
  return <div className="tabs" role="tablist" aria-label="View"><span className="sr-only">Selected: {active}</span>{options.map((option) => <button type="button" role="tab" aria-selected={active === option} key={option} onClick={() => { const next = new URLSearchParams(params); next.set('tab', option); next.delete('cursor'); setParams(next); }}>{option}</button>)}</div>;
}

export function useDetailNavigation(prefix: string, field: string) {
  const navigate = useNavigate();
  return (row: JsonRecord) => { const id = row[field]; if (id != null) navigate(`${prefix}/${encodeURIComponent(String(id))}`); };
}
