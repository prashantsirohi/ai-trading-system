import { loadEnv } from 'vite';
import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const useMock = env.VITE_USE_MOCK_API === 'true' || env.VITE_USE_MOCK_API === '1';
  const executionProxyTarget = env.VITE_EXECUTION_PROXY_TARGET || 'http://127.0.0.1:8090';
  const phase4ProxyTarget = env.VITE_PHASE4_PROXY_TARGET || 'http://127.0.0.1:8765';
  // GitHub Pages serves from a repo subpath; VITE_BASE_URL sets it at build time.
  const base = env.VITE_BASE_URL || '/';

  return {
    test: { environment: 'jsdom', setupFiles: './src/test/setup.ts', exclude: ['tests/e2e/**', 'node_modules/**'] },
    base,
    plugins: [react()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    server: {
      // Skip the backend proxy entirely when running in mock mode so that
      // stray /api/* requests surface as 404s rather than ECONNREFUSED noise.
      proxy: useMock
        ? {}
        : {
            '/api/v1': {
              target: phase4ProxyTarget,
              changeOrigin: true,
            },
            '/api': {
              target: executionProxyTarget,
              changeOrigin: true,
            },
          },
    },
  };
});
