import { loadEnv } from 'vite';
import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const repositoryEnv = loadEnv(mode, path.resolve(__dirname, '../../..'), '');
  const useMock = env.VITE_USE_MOCK_API === 'true' || env.VITE_USE_MOCK_API === '1';
  const executionProxyTarget = process.env.VITE_EXECUTION_PROXY_TARGET || env.VITE_EXECUTION_PROXY_TARGET || 'http://127.0.0.1:8090';
  const executionProxyKey = process.env.EXECUTION_API_KEY || repositoryEnv.EXECUTION_API_KEY || 'local-loopback-vite-proxy';
  const automaticLocalAccess = command === 'serve' && Boolean(executionProxyKey);
  const phase4ProxyTarget = process.env.VITE_PHASE4_PROXY_TARGET || env.VITE_PHASE4_PROXY_TARGET || 'http://127.0.0.1:8765';
  // GitHub Pages serves from a repo subpath; VITE_BASE_URL sets it at build time.
  const base = env.VITE_BASE_URL || '/';

  return {
    test: { environment: 'jsdom', setupFiles: './src/test/setup.ts', exclude: ['tests/e2e/**', 'node_modules/**'] },
    base,
    define: {
      'import.meta.env.VITE_LOCAL_NO_AUTH': JSON.stringify(automaticLocalAccess ? 'true' : 'false'),
    },
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
              configure: (proxy) => {
                proxy.on('proxyReq', (proxyRequest) => {
                  proxyRequest.setHeader('X-API-Key', executionProxyKey);
                });
              },
            },
          },
    },
  };
});
