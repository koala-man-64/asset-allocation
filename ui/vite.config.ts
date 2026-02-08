import { configDefaults, defineConfig } from 'vitest/config'
import { loadEnv } from 'vite'
import path from 'path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function normalizeProxyMountPath(raw: string): string {
  const trimmed = (raw || '').replace(/\/+$/, '')
  if (!trimmed || trimmed === '/') return ''
  if (trimmed === '/api') return ''
  if (trimmed.endsWith('/api')) {
    const withoutApi = trimmed.slice(0, -'/api'.length)
    return withoutApi && withoutApi !== '/' ? withoutApi : ''
  }
  return trimmed
}

function joinPath(prefix: string, path: string): string {
  const base = (prefix || '').replace(/\/+$/, '')
  const suffix = (path || '').startsWith('/') ? String(path || '') : `/${String(path || '')}`
  if (!base) return suffix || '/'
  return `${base}${suffix || '/'}`
}

function isTruthy(value: unknown): boolean {
  return ['1', 'true', 't', 'yes', 'y', 'on'].includes(String(value || '').trim().toLowerCase())
}

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
  const parsedApiProxyTarget = (() => {
    try {
      return new URL(apiProxyTarget)
    } catch {
      return null
    }
  })()
  const apiProxyOrigin = parsedApiProxyTarget ? parsedApiProxyTarget.origin : apiProxyTarget
  const apiProxyMountPath = normalizeProxyMountPath(parsedApiProxyTarget?.pathname || '')

  const apiRootPrefixRaw = env.API_ROOT_PREFIX || process.env.API_ROOT_PREFIX || ''
  const apiRootPrefix = (() => {
    const value = String(apiRootPrefixRaw || '').trim()
    if (!value || value === '/') return ''
    const trimmed = value.replace(/^\/+/, '').replace(/\/+$/, '')
    return trimmed ? `/${trimmed}` : ''
  })()
  const apiRootPrefixRe = apiRootPrefix ? new RegExp(`^${escapeRegExp(apiRootPrefix)}`) : null
  const proxyConfigJs = isTruthy(env.VITE_PROXY_CONFIG_JS || process.env.VITE_PROXY_CONFIG_JS)

  return {
    test: {
      globals: true,
      environment: 'jsdom',
      setupFiles: './src/test/setup.ts',
      css: false,
      exclude: [...configDefaults.exclude, '**/.pnpm-store/**'],
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
          target: apiProxyOrigin,
          changeOrigin: true,
          ws: true,
          rewrite: (path) => joinPath(apiProxyMountPath, path),
        },
        ...(proxyConfigJs
          ? {
              '/config.js': {
                target: apiProxyOrigin,
                changeOrigin: true,
              },
            }
          : {}),
        ...(apiRootPrefix
          ? {
              [`${apiRootPrefix}/api`]: {
                target: apiProxyOrigin,
                changeOrigin: true,
                ws: true,
                rewrite: (path) => {
                  const stripped = apiRootPrefixRe ? path.replace(apiRootPrefixRe, '') : path
                  return joinPath(apiProxyMountPath, stripped)
                },
              },
              ...(proxyConfigJs
                ? {
                    [`${apiRootPrefix}/config.js`]: {
                      target: apiProxyOrigin,
                      changeOrigin: true,
                      rewrite: (path) => (apiRootPrefixRe ? path.replace(apiRootPrefixRe, '') : path),
                    },
                  }
                : {}),
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
