/**
 * Watchlist & Alerts storage (Quantis proposal #02).
 *
 * Entries and rules persist to ``localStorage``; fired-alert events are
 * ephemeral (reset on page load) until a backend alerts feed endpoint
 * exists.
 */

export type AlertKind = 'score' | 'breakout' | 'pattern' | 'pipeline' | 'drawdown';
export type AlertSeverity = 'info' | 'warn' | 'critical';
export type AlertCooldown = '5m' | '1h' | '1d' | 'never';
export type AlertChannel = 'in-app' | 'slack' | 'email';

export interface AlertRule {
  id: string;
  kind: AlertKind;
  label: string;
  threshold?: number;
  severity: AlertSeverity;
  cooldown: AlertCooldown;
  channels: AlertChannel[];
  enabled: boolean;
}

export interface WatchlistEntry {
  symbol: string;
  addedAt: string;
  rules: AlertRule[];
}

export interface AlertFiredEvent {
  id: string;
  severity: AlertSeverity;
  ts: string;
  ruleName: string;
  symbols: string[];
  reason: string;
  muted: boolean;
  snoozedUntil?: string;
}

const STORAGE_KEY = 'quantis.watchlist.v1';

function isBrowser(): boolean {
  return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
}

export function loadWatchlist(): WatchlistEntry[] {
  if (!isBrowser()) return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as WatchlistEntry[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function saveWatchlist(entries: WatchlistEntry[]): void {
  if (!isBrowser()) return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(entries));
  } catch {
    // best-effort
  }
}

export function makeRule(
  kind: AlertKind,
  threshold?: number,
): AlertRule {
  const labels: Record<AlertKind, string> = {
    score: threshold != null ? `Score > ${threshold.toFixed(2)}` : 'Score threshold',
    breakout: 'Breakout confirmed',
    pattern: 'Pattern detected',
    pipeline: 'Pipeline failed / degraded',
    drawdown: threshold != null ? `DD > ${threshold}%` : 'Drawdown limit',
  };
  return {
    id: `rule-${Date.now().toString(36)}`,
    kind,
    label: labels[kind],
    threshold,
    severity: 'warn',
    cooldown: '1h',
    channels: ['in-app'],
    enabled: true,
  };
}
