import type { ApiEnvelope, ApiErrorBody } from './types';
import { Phase4ApiError } from './types';

export type AuthMode = 'bearer' | 'api-key';

const configuredBase = (import.meta.env.VITE_PHASE4_API_BASE_URL as string | undefined)?.trim() ?? '';
export const DEFAULT_POLL_SECONDS = Math.max(
  0,
  Number(import.meta.env.VITE_PHASE4_DEFAULT_POLL_SECONDS ?? 60) || 60,
);
export const CONFIGURED_AUTH_MODE: AuthMode =
  String(import.meta.env.VITE_PHASE4_API_AUTH_MODE ?? 'bearer').toLowerCase() === 'api-key'
    ? 'api-key'
    : 'bearer';
export const CONFIGURED_API_KEY = String(import.meta.env.VITE_PHASE4_API_KEY ?? '');

function apiBase(): string {
  if (!configuredBase) return '';
  const parsed = new URL(configuredBase, window.location.origin);
  if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error('Unsupported Phase 4 API URL');
  return parsed.href.replace(/\/$/, '');
}

function requestId(): string {
  return globalThis.crypto?.randomUUID?.() ?? `phase4-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

const etags = new Map<string, { tag: string; value: ApiEnvelope<unknown> }>();

export interface GetOptions {
  credential?: string;
  authMode?: AuthMode;
  signal?: AbortSignal;
  timeoutMs?: number;
  retries?: number;
}

export async function getPhase4<T>(path: string, options: GetOptions = {}): Promise<ApiEnvelope<T>> {
  if (!path.startsWith('/api/v1/')) throw new Error('Phase 4 requests must use /api/v1');
  const credential = options.credential?.trim();
  const headers = new Headers({ Accept: 'application/json', 'X-Request-ID': requestId() });
  if (credential) {
    if ((options.authMode ?? CONFIGURED_AUTH_MODE) === 'api-key') headers.set('X-API-Key', credential);
    else headers.set('Authorization', `Bearer ${credential}`);
  }
  const cached = etags.get(path);
  if (cached) headers.set('If-None-Match', cached.tag);

  const timeout = AbortSignal.timeout(options.timeoutMs ?? 12_000);
  const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
  const attempts = Math.min(2, Math.max(0, options.retries ?? 1)) + 1;
  let lastError: unknown;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetch(`${apiBase()}${path}`, { method: 'GET', headers, signal });
      if (response.status === 304 && cached) return cached.value as ApiEnvelope<T>;
      if (!response.ok) {
        const fallback: ApiErrorBody = {
          code: `HTTP_${response.status}`,
          message: response.statusText || 'Phase 4 API request failed',
          request_id: response.headers.get('X-Request-ID') ?? undefined,
        };
        const body = (await response.json().catch(() => fallback)) as ApiErrorBody;
        throw new Phase4ApiError(response.status, body);
      }
      const value = (await response.json()) as ApiEnvelope<T>;
      const tag = response.headers.get('ETag');
      if (tag) etags.set(path, { tag, value: value as ApiEnvelope<unknown> });
      return value;
    } catch (error) {
      lastError = error;
      const status = error instanceof Phase4ApiError ? error.status : 0;
      if (attempt + 1 >= attempts || ![0, 429, 503].includes(status)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
    }
  }
  throw lastError;
}

export const phase4Client = Object.freeze({ get: getPhase4 });
