/**
 * Common API Type Definitions
 */

export interface ApiResponse<T> {
    data: T;
    status: number;
    message?: string;
}

export interface ApiError {
    status: number;
    message: string;
    code?: string;
    details?: unknown;
}

export interface PaginatedResponse<T> {
    items: T[];
    total: number;
    limit: number;
    offset: number;
}
