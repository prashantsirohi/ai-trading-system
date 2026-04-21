import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? [['list'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? 'http://127.0.0.1:4173',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  webServer: {
    command: `npm run dev -- --host 127.0.0.1 --port ${process.env.PLAYWRIGHT_PORT ?? '4173'}`,
    port: Number(process.env.PLAYWRIGHT_PORT ?? '4173'),
    reuseExistingServer: false,
    timeout: 120000,
    env: {
      VITE_USE_MOCK_API: process.env.PLAYWRIGHT_USE_MOCK_API ?? 'true',
      VITE_EXECUTION_API_BASE_URL: process.env.PLAYWRIGHT_API_BASE_URL ?? 'http://127.0.0.1:8090',
      VITE_EXECUTION_API_KEY: process.env.PLAYWRIGHT_API_KEY ?? 'local-dev-key',
    },
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
