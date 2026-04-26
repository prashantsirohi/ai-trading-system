/**
 * Saved-view persistence for the Ranking filter rail (proposal #01).
 *
 * Filter state lives in component state; user-defined views are persisted
 * to ``localStorage`` keyed per browser. Seed views are not stored — they
 * are merged in at read-time so a schema bump can ship new defaults.
 */
export type Tier = 'A' | 'B' | 'C';

export interface RankingFilterState {
  scoreRange: [number, number];
  rsRange: [number, number];
  tiers: Tier[];
  sectors: string[];
  breakoutOnly: boolean;
  hasPatternOnly: boolean;
}

export interface SavedView {
  id: string;
  name: string;
  state: RankingFilterState;
  builtin?: boolean;
}

export const SCORE_BOUNDS: [number, number] = [0, 10];
export const RS_BOUNDS: [number, number] = [0, 100];

export const DEFAULT_FILTER_STATE: RankingFilterState = {
  scoreRange: [...SCORE_BOUNDS] as [number, number],
  rsRange: [...RS_BOUNDS] as [number, number],
  tiers: [],
  sectors: [],
  breakoutOnly: false,
  hasPatternOnly: false,
};

export const SEED_SAVED_VIEWS: SavedView[] = [
  { id: 'all', name: 'All', state: DEFAULT_FILTER_STATE, builtin: true },
  {
    id: 'tier-a-breakouts',
    name: 'Tier A breakouts',
    builtin: true,
    state: { ...DEFAULT_FILTER_STATE, tiers: ['A'], breakoutOnly: true },
  },
  {
    id: 'patterns-active',
    name: 'Patterns active',
    builtin: true,
    state: { ...DEFAULT_FILTER_STATE, hasPatternOnly: true },
  },
  {
    id: 'high-score',
    name: 'Score ≥ 7.5',
    builtin: true,
    state: { ...DEFAULT_FILTER_STATE, scoreRange: [7.5, 10] },
  },
];

const STORAGE_KEY = 'quantis.ranking.savedViews.v1';

function isBrowser(): boolean {
  return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
}

export function loadCustomViews(): SavedView[] {
  if (!isBrowser()) return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as SavedView[];
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((v) => v && typeof v.id === 'string' && typeof v.name === 'string');
  } catch {
    return [];
  }
}

export function saveCustomViews(views: SavedView[]): void {
  if (!isBrowser()) return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(views));
  } catch {
    // localStorage unavailable / quota exceeded — best-effort persistence.
  }
}

export function statesEqual(a: RankingFilterState, b: RankingFilterState): boolean {
  return (
    a.scoreRange[0] === b.scoreRange[0] &&
    a.scoreRange[1] === b.scoreRange[1] &&
    a.rsRange[0] === b.rsRange[0] &&
    a.rsRange[1] === b.rsRange[1] &&
    a.breakoutOnly === b.breakoutOnly &&
    a.hasPatternOnly === b.hasPatternOnly &&
    sameSet(a.tiers, b.tiers) &&
    sameSet(a.sectors, b.sectors)
  );
}

function sameSet<T extends string>(a: T[], b: T[]): boolean {
  if (a.length !== b.length) return false;
  const setB = new Set(b);
  return a.every((v) => setB.has(v));
}

export function isDefaultState(state: RankingFilterState): boolean {
  return statesEqual(state, DEFAULT_FILTER_STATE);
}
