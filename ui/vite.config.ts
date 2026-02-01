import { defineConfig } from 'vitest/config'
import { loadEnv } from 'vite'
import path from 'path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  // Load env from parent directory (../.env)
  const env = loadEnv(mode, path.resolve(__dirname, '..'), '')

  const portStr = env.VITE_PORT || process.env.VITE_PORT
  if (!portStr) {
    throw new Error('VITE_PORT is required in .env file or environment variables')
  }
  const parsedPort = Number(portStr)
  if (!Number.isFinite(parsedPort)) {
    throw new Error(`VITE_PORT must be a number (received: ${portStr})`)
  }
  const serverPort = parsedPort

  const apiProxyTarget = env.VITE_API_PROXY_TARGET || process.env.VITE_API_PROXY_TARGET || 'http://127.0.0.1:8000'
  const apiRootPrefixRaw = env.API_ROOT_PREFIX || process.env.API_ROOT_PREFIX || ''
  const apiRootPrefix = (() => {
    const value = String(apiRootPrefixRaw || '').trim()
    if (!value || value === '/') return ''
    const trimmed = value.replace(/^\/+/, '').replace(/\/+$/, '')
    return trimmed ? `/${trimmed}` : ''
  })()

  return {
    test: {
      globals: true,
      environment: 'jsdom',
      setupFiles: './src/test/setup.ts',
      css: false,
    },
    plugins: [
      // The React and Tailwind plugins are both required for Make, even if
      // Tailwind is not being actively used – do not remove them
      react(),
      tailwindcss(),
    ],
    server: {
      port: serverPort,
      strictPort: true,
      proxy: {
        '/api': {
          target: apiProxyTarget,
          changeOrigin: true,
          ws: true,
        },
        '/config.js': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        ...(apiRootPrefix
          ? {
              [`${apiRootPrefix}/api`]: {
                target: apiProxyTarget,
                changeOrigin: true,
                ws: true,
              },
              [`${apiRootPrefix}/config.js`]: {
                target: apiProxyTarget,
                changeOrigin: true,
              },
            }
          : {}),
      },
    },
    resolve: {
      alias: {
        // Alias @ to the src directory
        '@': path.resolve(__dirname, './src'),
      },
    },
  }
})
