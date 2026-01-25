import React from 'react';
import { CheckSquare, AlertOctagon, AlertTriangle, Power, Info, XCircle, Database, Clock, Loader2 } from 'lucide-react';
import { StatusColors } from './StatusTokens';
import { Badge } from '@/app/components/ui/badge';

interface StatusConfig {
    bg: string;
    text: string;
    border: string;
    icon: React.ElementType;
    animation?: 'spin' | 'pulse';
}

/**
 * Returns a configuration object (color, icon) for a given status.
 * Uses "Industrial Utility" tokens.
 */
export const getStatusConfig = (status: string): StatusConfig => {
    switch (status?.toLowerCase()) {
        case 'healthy':
        case 'success':
            return { ...StatusColors.HEALTHY, icon: CheckSquare };
        case 'degraded':
        case 'warning':
        case 'stale':
            return { ...StatusColors.WARNING, icon: AlertTriangle };
        // Broaden critical/error matching
        case 'critical':
        case 'error':
        case 'failed':
            return { ...StatusColors.CRITICAL, icon: AlertOctagon };
        case 'running':
            // Use Loader2 + Spin for active running states
            return { ...StatusColors.NEUTRAL, icon: Loader2, animation: 'spin' };
        case 'pending':
            return { ...StatusColors.NEUTRAL, icon: Clock };
        default:
            return { ...StatusColors.NEUTRAL, icon: Power };
    }
};

/**
 * Legacy support for direct icon rendering if needed elsewhere, 
 * but primarily we use getStatusConfig now.
 */
export const getStatusIcon = (status: string) => {
    const config = getStatusConfig(status);
    // Map animation string to tailwind class
    const animClass = config.animation === 'spin' ? 'animate-spin' : '';

    return React.createElement(config.icon, {
        className: `h-4 w-4 ${animClass}`,
        style: { color: config.text }
    });
};

/**
 * Updated to use legacy Badge component but with new colors if possible,
 * or mapped to standard styles. Retaining basic Badge for backward compat 
 * if used outside new Overview.
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

export const formatTimeAgo = (timestamp?: string | null) => {
    if (!timestamp) return '--:--';
    const diff = Date.now() - new Date(timestamp).getTime();
    if (diff < 0) return '0s'; // Future clock skew protection
    if (diff < 60000) return `${Math.floor(diff / 1000)}s`;
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h`;
    return `${Math.floor(diff / 86400000)}d`;
};

// Re-export specific icons if needed by consumers
export const getSeverityIcon = (severity: string) => {
    switch (severity) {
        case 'critical': return React.createElement(XCircle, { className: "h-4 w-4 text-red-600" });
        case 'error': return React.createElement(Info, { className: "h-4 w-4 text-red-500" });
        case 'warning': return React.createElement(AlertTriangle, { className: "h-4 w-4 text-yellow-600" });
        case 'info': return React.createElement(Info, { className: "h-4 w-4 text-blue-600" });
        default: return React.createElement(Info, { className: "h-4 w-4 text-gray-400" });
    }
};

export const getJobTypeIcon = (jobType: string) => {
    return React.createElement(Database, { className: "h-4 w-4" });
};

export const formatTimestamp = (timestamp?: string | null) => {
    if (!timestamp) return '-';
    // Use the new compact format by default now for consistency
    return formatTimeAgo(timestamp);
};

export const formatDuration = (seconds?: number | null) => {
    if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return '-';
    const total = Math.max(0, Math.floor(seconds));
    if (total < 60) return `${total}s`;
    const minutes = Math.floor(total / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const remMinutes = minutes % 60;
    return remMinutes ? `${hours}h ${remMinutes}m` : `${hours}h`;
};

export const formatRecordCount = (count?: number | null) => {
    if (count === null || count === undefined || !Number.isFinite(count)) return '-';
    return new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(count);
};
