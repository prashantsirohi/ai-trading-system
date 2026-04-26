/**
 * Seed data for the Watchlist & Alerts demo.
 *
 * The entries match symbols in the ranking mock so the table can show live
 * prices/scores when wired to the ranking feed.
 */
import type { AlertFiredEvent, WatchlistEntry } from '@/lib/storage/watchlist';

export const watchlistSeedEntries: WatchlistEntry[] = [
  {
    symbol: 'BEL',
    addedAt: '2026-04-25T09:00:00Z',
    rules: [
      {
        id: 'rule-bel-1',
        kind: 'breakout',
        label: 'Breakout confirmed',
        severity: 'warn',
        cooldown: '1h',
        channels: ['in-app', 'slack'],
        enabled: true,
      },
      {
        id: 'rule-bel-2',
        kind: 'score',
        label: 'Score > 7.50',
        threshold: 7.5,
        severity: 'info',
        cooldown: '1d',
        channels: ['in-app'],
        enabled: true,
      },
    ],
  },
  {
    symbol: 'HAL',
    addedAt: '2026-04-25T09:05:00Z',
    rules: [
      {
        id: 'rule-hal-1',
        kind: 'score',
        label: 'Score > 7.00',
        threshold: 7.0,
        severity: 'info',
        cooldown: '1h',
        channels: ['in-app'],
        enabled: true,
      },
    ],
  },
  {
    symbol: 'RELIANCE',
    addedAt: '2026-04-24T14:30:00Z',
    rules: [
      {
        id: 'rule-rel-1',
        kind: 'pattern',
        label: 'Pattern detected',
        severity: 'warn',
        cooldown: '1h',
        channels: ['in-app', 'email'],
        enabled: true,
      },
    ],
  },
  {
    symbol: 'INFY',
    addedAt: '2026-04-24T11:00:00Z',
    rules: [],
  },
];

export const alertsFeedSeed: AlertFiredEvent[] = [
  {
    id: 'alert-1',
    severity: 'warn',
    ts: '2026-04-26T09:21:00Z',
    ruleName: 'Breakout confirmed',
    symbols: ['BEL'],
    reason: 'BEL closed above resistance on volume 2.1× ADV · tight_flag setup confirmed',
    muted: false,
  },
  {
    id: 'alert-2',
    severity: 'info',
    ts: '2026-04-26T09:13:00Z',
    ruleName: 'Score > 7.00',
    symbols: ['HAL'],
    reason: 'HAL composite score crossed 7.00 threshold (now 7.02)',
    muted: false,
  },
  {
    id: 'alert-3',
    severity: 'warn',
    ts: '2026-04-26T08:47:00Z',
    ruleName: 'Pattern detected',
    symbols: ['RELIANCE'],
    reason: 'Cup & Handle setup confirmed on RELIANCE · confidence 0.82',
    muted: false,
  },
  {
    id: 'alert-4',
    severity: 'info',
    ts: '2026-04-25T15:58:00Z',
    ruleName: 'Score > 7.50',
    symbols: ['BEL'],
    reason: 'BEL score reached 7.98 · above 7.50 threshold',
    muted: true,
  },
];
