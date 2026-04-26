/** Mock presence + audit-log data for Proposal #08. */

export interface OperatorPresence {
  initials: string;
  name: string;
  color: string;
  page: string | null; // null = idle
  idleMins: number;
}

export const OPERATORS: OperatorPresence[] = [
  { initials: 'MS', name: 'm.sharma', color: '#3b82f6', page: 'Pipeline',  idleMins: 2  },
  { initials: 'RI', name: 'r.iyer',   color: '#10b981', page: 'Ranking',   idleMins: 6  },
  { initials: 'AK', name: 'a.khan',   color: '#d97706', page: null,        idleMins: 18 },
];

export interface AuditEntry {
  ts: string;
  actor: string;
  kind: 'operator' | 'system';
  msg: string;
}

export const AUDIT_LOG: AuditEntry[] = [
  { ts: '09:24:02', actor: 'm.sharma', kind: 'operator', msg: 'retried Execute stage' },
  { ts: '09:23:14', actor: 'system',   kind: 'system',   msg: 'trust → degraded' },
  { ts: '09:21:08', actor: 'r.iyer',   kind: 'operator', msg: 'added BEL to Defence basket' },
  { ts: '09:20:42', actor: 'm.sharma', kind: 'operator', msg: 'set killswitch DISARMED' },
  { ts: '09:18:45', actor: 'm.sharma', kind: 'operator', msg: 'approved Shadow v0.7 (1 of 2)' },
  { ts: '09:17:31', actor: 'system',   kind: 'system',   msg: 'trust → trusted' },
  { ts: '09:16:10', actor: 'a.khan',   kind: 'operator', msg: 'added NTPC to watchlist' },
  { ts: '09:15:00', actor: 'system',   kind: 'system',   msg: 'pipeline started · run 1042' },
  { ts: '09:14:12', actor: 'a.khan',   kind: 'operator', msg: 'dismissed alert: RELIANCE score drop' },
  { ts: '09:12:05', actor: 'm.sharma', kind: 'operator', msg: 'published run 1041 → prod' },
  { ts: '09:10:00', actor: 'system',   kind: 'system',   msg: 'pipeline started · run 1041' },
  { ts: '09:08:44', actor: 'r.iyer',   kind: 'operator', msg: 'added HAL to watchlist' },
];

export interface RunSnapshot {
  runId: string;
  date: string;
  label: string;
}

/** Eight prior run snapshots for the time machine slider. */
export const RUN_SNAPSHOTS: RunSnapshot[] = [
  { runId: '1035', date: '2026-04-08', label: '04-08' },
  { runId: '1036', date: '2026-04-09', label: '04-09' },
  { runId: '1037', date: '2026-04-10', label: '04-10' },
  { runId: '1038', date: '2026-04-11', label: '04-11' },
  { runId: '1039', date: '2026-04-12', label: '04-12' },
  { runId: '1040', date: '2026-04-13', label: '04-13' },
  { runId: '1041', date: '2026-04-14', label: '04-14' },
  { runId: '1042', date: '2026-04-15', label: '04-15 · live' },
];
