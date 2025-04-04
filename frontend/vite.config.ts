import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',  // Backend-Adresse und Port
        changeOrigin: true,
        secure: false,
      }
    }
  },
  build: {
    sourcemap: true, // wichtig fürs Debuggen
  },
})
