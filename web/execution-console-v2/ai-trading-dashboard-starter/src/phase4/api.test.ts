import { afterEach, describe, expect, it, vi } from 'vitest';
import { getPhase4, phase4Client } from './api';
import { Phase4ApiError } from './types';

const envelope = { data: { ok: true }, meta: { request_id: 'req', generated_at: '2026-07-15T00:00:00Z', partial: false, limitations: [], lineage: [], lineage_meta: {}, freshness: { freshness_status: 'UNKNOWN', freshness_reasons: [] } } };

afterEach(() => vi.unstubAllGlobals());

describe('Phase 4 GET-only API client', () => {
  it('uses bearer authentication, request IDs, and GET', async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(envelope), { status: 200 }));
    vi.stubGlobal('fetch', fetcher);
    await getPhase4('/api/v1/system/readiness', { credential: 'secret', authMode: 'bearer' });
    const [, options] = fetcher.mock.calls[0];
    expect(options.method).toBe('GET');
    expect(options.headers.get('Authorization')).toBe('Bearer secret');
    expect(options.headers.get('X-Request-ID')).toBeTruthy();
  });

  it('uses the API key header and never places credentials in the URL', async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(envelope), { status: 200 }));
    vi.stubGlobal('fetch', fetcher);
    await getPhase4('/api/v1/system/version', { credential: 'do-not-leak', authMode: 'api-key' });
    const [url, options] = fetcher.mock.calls[0];
    expect(url).not.toContain('do-not-leak');
    expect(options.headers.get('X-API-Key')).toBe('do-not-leak');
  });

  it.each([401, 403, 404, 409])('returns a typed non-retryable %s error', async (status) => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ code: `E${status}`, message: 'safe', request_id: 'req' }), { status }));
    vi.stubGlobal('fetch', fetcher);
    await expect(getPhase4('/api/v1/system/version', { retries: 0 })).rejects.toMatchObject({ status, body: { request_id: 'req' } });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it.each([429, 503])('bounds transient retry for %s', async (status) => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ code: 'TRANSIENT', message: 'safe' }), { status }));
    vi.stubGlobal('fetch', fetcher);
    await expect(getPhase4('/api/v1/system/version', { retries: 1 })).rejects.toBeInstanceOf(Phase4ApiError);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it('reuses an ETag response on 304', async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(envelope), { status: 200, headers: { ETag: '"abc"' } }))
      .mockResolvedValueOnce(new Response(null, { status: 304 }));
    vi.stubGlobal('fetch', fetcher);
    await getPhase4('/api/v1/system/readiness?etag=test');
    const second = await getPhase4('/api/v1/system/readiness?etag=test');
    expect(second.data).toEqual({ ok: true });
    expect(fetcher.mock.calls[1][1].headers.get('If-None-Match')).toBe('"abc"');
  });

  it('rejects paths outside the Phase 4 boundary', async () => {
    await expect(getPhase4('/api/execution/orders')).rejects.toThrow('/api/v1');
  });

  it('exposes no mutation methods', () => {
    expect(Object.keys(phase4Client)).toEqual(['get']);
    expect((phase4Client as Record<string, unknown>).post).toBeUndefined();
  });
});
