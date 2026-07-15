import { defineConfig, devices } from '@playwright/test';

// Playwright enables colored worker output after loading this config. Avoid
// propagating a contradictory NO_COLOR marker into its child processes.
if (process.env.NO_COLOR) delete process.env.NO_COLOR;

const playwrightPort = Number(process.env.PLAYWRIGHT_PORT ?? '4173');
const phase4ApiPort = Number(process.env.PLAYWRIGHT_PHASE4_API_PORT ?? '8765');
const phase4ApiTarget = process.env.PLAYWRIGHT_PHASE4_API_BASE_URL ?? `http://127.0.0.1:${phase4ApiPort}`;
const realApi = process.env.PLAYWRIGHT_REAL_API === 'true';

export default defineConfig({
  testDir: './tests/e2e',
  testMatch: '**/phase4-*.spec.ts',
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? [['list'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? `http://127.0.0.1:${playwrightPort}`,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  webServer: [
    ...(realApi ? [{
      command: `cd ../../.. && PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api --fixture-profile small_fixture --host 127.0.0.1 --port ${phase4ApiPort}`,
      port: phase4ApiPort,
      reuseExistingServer: false,
      timeout: 120000,
      env: {
        PHASE4_API_KEY: process.env.PLAYWRIGHT_API_KEY ?? 'local-dev-key',
        PHASE4_API_AUTH_ENABLED: 'true',
        PHASE4_API_RATE_LIMIT_PER_MINUTE: '1000',
      },
    }] : []),
    {
      command: `npm run dev -- --host 127.0.0.1 --port ${playwrightPort}`,
      port: playwrightPort,
      reuseExistingServer: false,
      timeout: 120000,
      env: {
        VITE_USE_MOCK_API: realApi ? 'false' : (process.env.PLAYWRIGHT_USE_MOCK_API ?? 'true'),
        VITE_EXECUTION_API_BASE_URL: phase4ApiTarget,
        VITE_PHASE4_PROXY_TARGET: phase4ApiTarget,
        VITE_EXECUTION_API_KEY: process.env.PLAYWRIGHT_API_KEY ?? 'local-dev-key',
        VITE_PHASE4_API_BASE_URL: '',
        VITE_PHASE4_DEFAULT_POLL_SECONDS: '300',
      },
    },
  ],
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
