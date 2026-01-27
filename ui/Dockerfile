# Stage 1: Build the React application bundle.
FROM node:20-slim AS builder
WORKDIR /app

# Build-time defaults (can be overridden via `--build-arg`).
# These are baked into the bundle; runtime overrides should use /config.js.
ARG VITE_API_BASE_URL=/api
ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

# Install pnpm for reproducible builds.
RUN npm install -g pnpm

# Copy package management files to maximize build cache hits.
COPY package.json pnpm-lock.yaml ./

# Install dependencies (lockfile enforced).
RUN pnpm install --frozen-lockfile

# Copy source code into the build context.
COPY . .

# Build the application.
RUN pnpm run build

# Stage 2: Serve with Nginx (reverse proxy + static assets).
FROM nginx:alpine
WORKDIR /usr/share/nginx/html

# Remove default nginx static assets.
RUN rm -rf ./*

# Copy built artifacts from builder stage.
COPY --from=builder /app/dist .

# Nginx reverse proxy for UI + API (single origin).
# See ui/nginx.conf for /api and /config.js proxy rules.
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

# Run nginx in the foreground.
CMD ["nginx", "-g", "daemon off;"]
