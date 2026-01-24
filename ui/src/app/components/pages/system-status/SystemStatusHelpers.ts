import React from 'react';
import {
    Activity,
    AlertCircle,
    AlertTriangle,
    CheckCircle2,
    Clock,
    Database,
    Loader2,
    PlayCircle,
    TrendingUp,
    XCircle,
    Info
} from 'lucide-react';
import { Badge } from '@/app/components/ui/badge';

/**
 * Returns a React node (Icon) for a given status string.
 */
export const getStatusIcon = (status: string) => {
    switch (status) {
        case 'healthy':
        case 'success':
            return React.createElement(CheckCircle2, { className: "h-4 w-4 text-green-600" });
        case 'degraded':
        case 'stale':
        case 'warning':
            return React.createElement(AlertTriangle, { className: "h-4 w-4 text-yellow-600" });
        case 'critical':
        case 'error':
        case 'failed':
            return React.createElement(XCircle, { className: "h-4 w-4 text-red-600" });
        case 'running':
            return React.createElement(Loader2, { className: "h-4 w-4 text-blue-600 animate-spin" });
        case 'pending':
            return React.createElement(Clock, { className: "h-4 w-4 text-gray-400" });
        default:
            return React.createElement(AlertCircle, { className: "h-4 w-4 text-gray-400" });
    }
};

/**
 * Returns a styled Badge component for a given status.
 */
export const getStatusBadge = (status: string) => {
    const styles: Record<string, string> = {
        healthy: 'bg-green-100 text-green-800 hover:bg-green-100 border-green-200',
        success: 'bg-green-100 text-green-800 hover:bg-green-100 border-green-200',
        degraded: 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100 border-yellow-200',
        stale: 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100 border-yellow-200',
        warning: 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100 border-yellow-200',
        critical: 'bg-red-100 text-red-800 hover:bg-red-100 border-red-200',
        error: 'bg-red-100 text-red-800 hover:bg-red-100 border-red-200',
        failed: 'bg-red-100 text-red-800 hover:bg-red-100 border-red-200',
        running: 'bg-blue-100 text-blue-800 hover:bg-blue-100 border-blue-200',
        pending: 'bg-gray-100 text-gray-800 hover:bg-gray-100 border-gray-200'
    };

    return React.createElement(
        Badge,
        { variant: 'outline', className: `font-mono text-xs border ${styles[status] || 'bg-gray-100 text-gray-800 border-gray-200'}` },
        status.toUpperCase()
    );
};

/**
 * Returns a severity icon for Alerts.
 */
export const getSeverityIcon = (severity: string) => {
    switch (severity) {
        case 'critical':
            return React.createElement(XCircle, { className: "h-4 w-4 text-red-600" });
        case 'error':
            return React.createElement(AlertCircle, { className: "h-4 w-4 text-red-500" });
        case 'warning':
            return React.createElement(AlertTriangle, { className: "h-4 w-4 text-yellow-600" });
        case 'info':
            return React.createElement(Info, { className: "h-4 w-4 text-blue-600" });
        default:
            return React.createElement(Info, { className: "h-4 w-4 text-gray-400" });
    }
};

/**
 * Returns an icon for a specific Job Type.
 */
export const getJobTypeIcon = (jobType: string) => {
    switch (jobType) {
        case 'backtest':
            return React.createElement(TrendingUp, { className: "h-4 w-4" });
        case 'data-ingest':
            return React.createElement(Database, { className: "h-4 w-4" });
        case 'attribution':
            return React.createElement(Activity, { className: "h-4 w-4" });
        case 'risk-calc':
            return React.createElement(AlertTriangle, { className: "h-4 w-4" });
        case 'portfolio-build':
            return React.createElement(PlayCircle, { className: "h-4 w-4" });
        default:
            return React.createElement(Activity, { className: "h-4 w-4" });
    }
};

/**
 * Formats a duration in seconds to mins/secs.
 */
export const formatDuration = (seconds?: number) => {
    if (!seconds) return '-';
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}m ${secs}s`;
};

/**
 * Formats a timestamp into a relative "ago" string or date.
 */
export const formatTimestamp = (timestamp?: string | null) => {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
};

/**
 * Formats a raw number count to K/M suffixes.
 */
export const formatRecordCount = (count?: number) => {
    if (!count) return '-';
    if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(1)}M`;
    if (count >= 1_000) return `${(count / 1_000).toFixed(1)}K`;
    return count.toString();
};
