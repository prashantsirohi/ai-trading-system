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

  /** Vite-only — proxy target for Phase 4B /api/v1/* requests. */
  readonly VITE_PHASE4_PROXY_TARGET?: string;

  /** Optional absolute Phase 4A API base; empty uses the same-origin proxy. */
  readonly VITE_PHASE4_API_BASE_URL?: string;

  /** Optional disposable local fixture credential. Never use for deployment. */
  readonly VITE_PHASE4_API_KEY?: string;

  /** Phase 4A authentication header mode: bearer or api-key. */
  readonly VITE_PHASE4_API_AUTH_MODE?: string;

  /** Phase 4B query polling interval in seconds. */
  readonly VITE_PHASE4_DEFAULT_POLL_SECONDS?: string;

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
