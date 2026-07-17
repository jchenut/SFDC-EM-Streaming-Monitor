import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Proxy /api to the back-end so the browser talks to a single origin
// (avoids CORS and keeps EventSource on the same host during dev).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
    },
  },
});
