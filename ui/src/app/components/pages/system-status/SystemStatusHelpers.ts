import React from 'react';
import {
    CheckSquare, AlertOctagon, AlertTriangle,
    Power, Info, XCircle, Database, Clock, Loader2
} from 'lucide-react';
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

export const getJobTypeIcon = (_jobType: string) => {
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

const CRON_EXPRESSION_RE = /(^|\b)([\d*/,-]+)\s+([\d*/,-]+)\s+([\d*/,-]+)\s+([\d*/,-]+)\s+([\d*/,-]+)(\b|$)/;

const formatUtcTime = (hour24: number, minute: number) => {
    const hour = ((hour24 + 11) % 12) + 1;
    const period = hour24 >= 12 ? 'PM' : 'AM';
    const paddedMinute = String(minute).padStart(2, '0');
    return `${hour}:${paddedMinute} ${period}`;
};

const buildCronEnglish = (expression: string) => {
    const match = expression.trim().match(CRON_EXPRESSION_RE);
    if (!match) return '';

    const minute = match[2];
    const hour = match[3];
    const dayOfMonth = match[4];
    const month = match[5];
    const dayOfWeek = match[6];

    const minuteStep = minute.match(/^\*\/(\d+)$/);
    const hourStep = hour.match(/^\*\/(\d+)$/);
    const hourRange = hour.match(/^(\d{1,2})-(\d{1,2})$/);
    const hourList = hour.match(/^\d{1,2}(,\d{1,2})+$/);

    const minuteValue = Number(minute);
    const hourValue = Number(hour);
    const minuteIsNumber = Number.isFinite(minuteValue) && /^\d{1,2}$/.test(minute);
    const hourIsNumber = Number.isFinite(hourValue) && /^\d{1,2}$/.test(hour);

    const isDaily = dayOfMonth === '*' && month === '*' && dayOfWeek === '*';

    if (isDaily) {
        if (minuteStep && hour === '*') {
            return `Every ${minuteStep[1]} minutes UTC`;
        }

        if (hourStep && minuteIsNumber) {
            return `Every ${hourStep[1]} hours at :${String(minuteValue).padStart(2, '0')} UTC`;
        }

        if (hourRange && minuteIsNumber) {
            const startHour = Number(hourRange[1]);
            const endHour = Number(hourRange[2]);
            const startTime = formatUtcTime(startHour, minuteValue);
            const endTime = formatUtcTime(endHour, minuteValue);
            if (minuteValue === 0) {
                return `Hourly between ${startTime}–${endTime} UTC`;
            }
            return `Hourly at :${String(minuteValue).padStart(2, '0')} between ${startTime}–${endTime} UTC`;
        }

        if (hourList && minuteIsNumber) {
            const times = hour.split(',').map((value) => formatUtcTime(Number(value), minuteValue));
            return `Daily at ${times.join(', ')} UTC`;
        }

        if (hourIsNumber && minuteIsNumber) {
            return `Daily at ${formatUtcTime(hourValue, minuteValue)} UTC`;
        }

        if (hour === '*' && minuteIsNumber) {
            return `Every hour at :${String(minuteValue).padStart(2, '0')} UTC`;
        }
    }

    return '';
};

export const formatSchedule = (schedule?: string | null) => {
    if (!schedule) return '-';
    const raw = String(schedule).trim();
    if (!raw) return '-';

    const cronMatch = raw.match(CRON_EXPRESSION_RE);
    if (!cronMatch) return raw;

    const cronExpression = `${cronMatch[2]} ${cronMatch[3]} ${cronMatch[4]} ${cronMatch[5]} ${cronMatch[6]}`;
    return buildCronEnglish(cronExpression) || raw;
};

export const normalizeAzurePortalUrl = (value?: string | null) => {
    if (!value) {
        return '';
    }
    const trimmed = String(value).trim();
    if (!trimmed) {
        return '';
    }

    if (/^https?:\/\//i.test(trimmed)) {
        return trimmed;
    }
    if (/^portal\.azure\.com/i.test(trimmed)) {
        return `https://${trimmed}`;
    }
    if (trimmed.startsWith('#')) {
        return `https://portal.azure.com/${trimmed}`;
    }

    const resourceId = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
    return `https://portal.azure.com/#resource${resourceId}`;
};

export const normalizeAzureJobName = (value?: string | null) => {
    if (!value) {
        return '';
    }
    let trimmed = String(value).trim();
    if (!trimmed) {
        return '';
    }

    // Accept common Azure portal/resource URL shapes and extract the job name segment.
    const match = trimmed.match(/\/jobs\/([^/?#]+)/);
    if (match) {
        try {
            trimmed = decodeURIComponent(match[1]);
        } catch {
            trimmed = match[1];
        }
    }

    return trimmed.toLowerCase().replace(/_/g, '-');
};

export const getAzurePortalUrl = (azureId?: string | null) => {
    return normalizeAzurePortalUrl(azureId);
};

export const getAzureJobExecutionsUrl = (jobPortalUrl?: string | null) => {
    const normalized = normalizeAzurePortalUrl(jobPortalUrl);
    if (!normalized) {
        return '';
    }
    const trimmed = String(normalized).trim();
    if (!trimmed) {
        return '';
    }

    // Container App Job portal URLs are generated as:
    // https://portal.azure.com/#resource/.../providers/Microsoft.App/jobs/<job>/overview
    // The execution history lives under the same resource path with `/executions`.
    const overviewMatch = trimmed.match(/\/overview([?#].*)?$/);
    if (overviewMatch) {
        return trimmed.replace(/\/overview([?#].*)?$/, '/executions$1');
    }

    // If we only have a base resource URL, append `/executions`.
    if (/\/providers\/Microsoft\.App\/jobs\/[^/]+$/.test(trimmed)) {
        return `${trimmed}/executions`;
    }

    return trimmed;
};
