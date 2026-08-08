import { afterEach, describe, expect, it, vi } from 'vitest';
import { journalApi } from './api';

describe('journalApi', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('uses the in-memory credential only in X-API-Key', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ items: [] }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    }));
    vi.stubGlobal('fetch', fetchMock);
    await journalApi.get('/api/trade-journal/accounts', 'local-secret');
    const [url, options] = fetchMock.mock.calls[0];
    expect(String(url)).not.toContain('local-secret');
    expect(new Headers(options.headers).get('X-API-Key')).toBe('local-secret');
  });

  it('rejects requests outside the journal route family', async () => {
    await expect(journalApi.get('/api/v1/positions', 'credential')).rejects.toThrow(
      'Journal requests must use /api/trade-journal',
    );
  });
});
