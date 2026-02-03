import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const workerUrl = "https://mahoraga.leviath.workers.dev/"

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: workerUrl,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '/agent'),
      },
    },
  },
})