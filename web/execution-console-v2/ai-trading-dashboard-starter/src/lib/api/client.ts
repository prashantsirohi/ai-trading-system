const API_BASE_URL = 'http://127.0.0.1:8090';
export const EXECUTION_API_KEY = 'local-dev-key';

// Use real API by default - only mock when explicitly set
export const USE_MOCK_API = false;

async function requestJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
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