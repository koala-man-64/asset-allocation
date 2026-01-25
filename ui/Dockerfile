# Stage 1: Build the React application
FROM node:20-slim AS builder
WORKDIR /app

# Build-time defaults (can be overridden via `--build-arg`).
ARG VITE_API_BASE_URL=/api
ARG VITE_BACKTEST_API_BASE_URL=/api
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}
ENV VITE_BACKTEST_API_BASE_URL=${VITE_BACKTEST_API_BASE_URL}

# Install pnpm
RUN npm install -g pnpm

# Copy package management files
COPY package.json pnpm-lock.yaml ./

# Install dependencies
# Using --frozen-lockfile to ensure reproducible builds
RUN pnpm install --frozen-lockfile

# Copy source code
COPY . .

# Build the application
RUN pnpm run build

# Stage 2: Serve with Nginx
FROM nginx:alpine
WORKDIR /usr/share/nginx/html

# Remove default nginx static assets
RUN rm -rf ./*

# Copy built artifacts from builder stage
COPY --from=builder /app/dist .

# Nginx reverse proxy for UI + API (single origin).
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
