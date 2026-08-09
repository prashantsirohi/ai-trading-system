const configuredBase = (import.meta.env.VITE_EXECUTION_API_BASE_URL as string | undefined)?.trim() ?? '';

function baseUrl(): string {
  if (!configuredBase) return '';
  const parsed = new URL(configuredBase, window.location.origin);
  if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error('Unsupported execution API URL');
  return parsed.href.replace(/\/$/, '');
}

async function request<T>(path: string, credential: string, init: RequestInit = {}): Promise<T> {
  if (!path.startsWith('/api/trade-journal/')) throw new Error('Journal requests must use /api/trade-journal');
  const headers = new Headers(init.headers);
  if (credential !== 'local-no-auth') headers.set('X-API-Key', credential);
  if (!(init.body instanceof FormData)) headers.set('Accept', 'application/json');
  const response = await fetch(`${baseUrl()}${path}`, { ...init, headers });
  const body = await response.json().catch(() => ({ detail: response.statusText }));
  if (!response.ok) throw new Error(String(body.detail ?? 'Journal API request failed'));
  return body as T;
}

export const journalApi = {
  get: <T>(path: string, credential: string) => request<T>(path, credential),
  form: <T>(path: string, credential: string, form: FormData) => request<T>(path, credential, { method: 'POST', body: form }),
};
