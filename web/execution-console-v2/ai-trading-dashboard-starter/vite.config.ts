import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const useMock = env.VITE_USE_MOCK_API === 'true' || env.VITE_USE_MOCK_API === '1';
  const proxyTarget = env.VITE_EXECUTION_PROXY_TARGET || 'http://127.0.0.1:8090';

  return {
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
            '/api': {
              target: proxyTarget,
              changeOrigin: true,
            },
          },
    },
  };
});
