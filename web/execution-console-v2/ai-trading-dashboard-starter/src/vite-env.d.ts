/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Toggle the in-memory mock API instead of calling the FastAPI backend. */
  readonly VITE_USE_MOCK_API?: string;

  /**
   * Absolute base URL for the FastAPI execution backend. Empty string means
   * "use the dev-server proxy" (request paths stay relative).
   */
  readonly VITE_EXECUTION_API_BASE_URL?: string;

  /** API key sent as the `x-api-key` header on every backend request. */
  readonly VITE_EXECUTION_API_KEY?: string;

  /** Vite-only — proxy target for /api/* during `vite dev`. */
  readonly VITE_EXECUTION_PROXY_TARGET?: string;

  /** Default react-query refetchInterval (ms). 0 disables auto-refresh. */
  readonly VITE_DEFAULT_REFETCH_INTERVAL_MS?: string;

  /**
   * Execution view mode — "preview" or "live". Cosmetic until a backend
   * execution endpoint lands; the trust pipeline enforces gating regardless.
   */
  readonly VITE_EXECUTION_MODE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
