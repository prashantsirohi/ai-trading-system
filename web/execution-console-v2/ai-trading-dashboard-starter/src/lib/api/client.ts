/**
 * Single source of truth for execution-console runtime configuration.
 *
 * All values are read from Vite's `import.meta.env.*` and fall back to dev
 * defaults so the UI still boots when the operator forgets to copy
 * `.env.example` to `.env.local`.
 */

function readBoolean(value: string | undefined, defaultValue: boolean): boolean {
  if (value === undefined || value === '') {
    return defaultValue;
  }
  return value === 'true' || value === '1';
}

function readNumber(value: string | undefined, defaultValue: number): number {
  if (value === undefined || value === '') {
    return defaultValue;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : defaultValue;
}

function readString(value: string | undefined, defaultValue: string): string {
  if (value === undefined) {
    return defaultValue;
  }
  return value;
}

export const API_BASE_URL = readString(
  import.meta.env.VITE_EXECUTION_API_BASE_URL,
  '',
);

export const EXECUTION_API_KEY = readString(
  import.meta.env.VITE_EXECUTION_API_KEY,
  'local-dev-key',
);

export const USE_MOCK_API = readBoolean(import.meta.env.VITE_USE_MOCK_API, false);

export const DEFAULT_REFETCH_INTERVAL_MS = readNumber(
  import.meta.env.VITE_DEFAULT_REFETCH_INTERVAL_MS,
  60_000,
);

export type ExecutionMode = 'preview' | 'live';

export const EXECUTION_MODE: ExecutionMode =
  readString(import.meta.env.VITE_EXECUTION_MODE, 'preview').toLowerCase() === 'live'
    ? 'live'
    : 'preview';

async function requestJson<T>(path: string): Promise<T> {
  const url = API_BASE_URL ? `${API_BASE_URL}${path}` : path;
  const response = await fetch(url, {
    headers: { 'x-api-key': EXECUTION_API_KEY },
  });

  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${path}`);
  }

  return (await response.json()) as T;
}

export async function fetchDashboardJson<T>(path: string, fallback: T): Promise<T> {
  if (USE_MOCK_API) {
    return fallback;
  }

  try {
    return await requestJson<T>(path);
  } catch (error) {
    console.warn(`[execution-console-v2] falling back to mock for ${path}`, error);
    return fallback;
  }
}

export async function fetchDashboardJsonStrict<T>(path: string, fallback: T): Promise<T> {
  if (USE_MOCK_API) {
    return fallback;
  }
  return requestJson<T>(path);
}
