/* global RequestInit */
import { config } from '@/config';

/**
 * Core API Client for unified backend communication.
 * Handles base URL configuration, common headers, and error normalization.
 */
class ApiClient {
    private baseUrl: string;

    constructor(baseUrl: string) {
        this.baseUrl = baseUrl.replace(/\/$/, '');
    }

    /**
     * Generic request method wrapper
     */
    async request<T>(path: string, options?: RequestInit): Promise<T> {
        const url = `${this.baseUrl}${path.startsWith('/') ? path : `/${path}`}`;

        const headers = new Headers(options?.headers);
        if (!headers.has('Content-Type')) {
            headers.set('Content-Type', 'application/json');
        }
        if (!headers.has('Accept')) {
            headers.set('Accept', 'application/json');
        }

        // TODO: Add Auth Token injection here if needed
        // const token = authService.getToken();
        // if (token) headers.set('Authorization', `Bearer ${token}`);

        const response = await fetch(url, {
            ...options,
            headers,
        });

        if (!response.ok) {
            let errorMessage = `API Error: ${response.status} ${response.statusText}`;
            try {
                const errorData = await response.json();
                errorMessage = errorData.detail || errorData.message || errorMessage;
            } catch {
                // Ignore JSON parse error on failure
            }
            throw new Error(errorMessage);
        }

        // Handle 204 No Content
        if (response.status === 204) {
            return {} as T;
        }

        return response.json();
    }

    /**
     * Helper HTTP verb methods
     */
    get<T>(path: string, options?: RequestInit): Promise<T> {
        return this.request<T>(path, { ...options, method: 'GET' });
    }

    post<T>(path: string, body?: unknown, options?: RequestInit): Promise<T> {
        return this.request<T>(path, {
            ...options,
            method: 'POST',
            body: JSON.stringify(body),
        });
    }

    put<T>(path: string, body?: unknown, options?: RequestInit): Promise<T> {
        return this.request<T>(path, {
            ...options,
            method: 'PUT',
            body: JSON.stringify(body),
        });
    }

    delete<T>(path: string, options?: RequestInit): Promise<T> {
        return this.request<T>(path, { ...options, method: 'DELETE' });
    }
}

export const apiClient = new ApiClient(config.apiBaseUrl);
