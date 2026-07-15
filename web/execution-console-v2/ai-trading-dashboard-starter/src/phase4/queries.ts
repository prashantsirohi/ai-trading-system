import { useQuery } from '@tanstack/react-query';
import { getPhase4, DEFAULT_POLL_SECONDS } from './api';
import { useAuth } from './auth';
import type { ApiEnvelope } from './types';

export function usePhase4Query<T>(path: string, options: { poll?: boolean; enabled?: boolean } = {}) {
  const auth = useAuth();
  return useQuery<ApiEnvelope<T>>({
    queryKey: ['phase4', path, auth.authMode, Boolean(auth.credential)],
    queryFn: ({ signal }) => getPhase4<T>(path, { credential: auth.credential, authMode: auth.authMode, signal }),
    enabled: options.enabled ?? true,
    retry: false,
    staleTime: options.poll ? DEFAULT_POLL_SECONDS * 500 : 60_000,
    refetchInterval: options.poll ? DEFAULT_POLL_SECONDS * 1000 : false,
    refetchIntervalInBackground: false,
  });
}

export function withParams(path: string, params: URLSearchParams, allowed: string[]): string {
  const query = new URLSearchParams();
  for (const key of allowed) {
    const value = params.get(key);
    if (value) query.set(key, value);
  }
  const suffix = query.toString();
  return suffix ? `${path}?${suffix}` : path;
}
