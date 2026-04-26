/** Mock news & fills feed for the Symbol page (Proposal #10). */

export type NewsKind = 'ALERT' | 'NEWS' | 'FILL' | 'SYSTEM';

export interface NewsEntry {
  ts: string;
  kind: NewsKind;
  headline: string;
}

const GENERIC_ENTRIES: NewsEntry[] = [
  { ts: '09:21', kind: 'ALERT', headline: 'Breakout confirmed · added to basket' },
  { ts: '04-22', kind: 'NEWS',  headline: 'Q4 order book +18% YoY · Reuters' },
  { ts: '04-18', kind: 'FILL',  headline: 'BUY 3,070 sh @ 318.40 · VWAP / 30m' },
  { ts: '04-14', kind: 'SYSTEM',headline: 'Score upgraded A → 7.98 · run 1039' },
  { ts: '04-12', kind: 'NEWS',  headline: 'Institutional block trade flagged · NSE data' },
  { ts: '04-08', kind: 'ALERT', headline: 'RS rank entered top-20 universe' },
];

const OVERRIDES: Record<string, NewsEntry[]> = {
  BEL: [
    { ts: '09:21', kind: 'ALERT', headline: 'Breakout confirmed · added to Defence basket' },
    { ts: '04-22', kind: 'NEWS',  headline: 'Q4 order book +18% YoY · Reuters' },
    { ts: '04-18', kind: 'FILL',  headline: 'BUY 3,070 sh @ 318.40 · VWAP / 30m' },
    { ts: '04-12', kind: 'NEWS',  headline: 'MoD contract award notification' },
  ],
  RELIANCE: [
    { ts: '09:15', kind: 'ALERT', headline: 'Volume 2.1× ADV — watching for breakout' },
    { ts: '04-21', kind: 'NEWS',  headline: 'Jio 5G subscriber milestone · Bloomberg' },
    { ts: '04-17', kind: 'FILL',  headline: 'BUY 340 sh @ 2891.20 · DMA · limit' },
    { ts: '04-10', kind: 'NEWS',  headline: 'Retail segment Q4 outperformance · NSE filing' },
  ],
  INFY: [
    { ts: '09:05', kind: 'ALERT', headline: 'IT sector cap headroom warning' },
    { ts: '04-20', kind: 'NEWS',  headline: 'Deal wins accelerating · FactSet consensus' },
    { ts: '04-15', kind: 'FILL',  headline: 'BUY 605 sh @ 1601.80 · IS / 45m' },
    { ts: '04-09', kind: 'SYSTEM',headline: 'RS rank improved 32→28 · run 1036' },
  ],
};

export function getSymbolNews(symbol: string): NewsEntry[] {
  return OVERRIDES[symbol] ?? GENERIC_ENTRIES;
}
