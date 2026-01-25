/**
 * Industrial Utility Design Tokens
 * Aesthetic: High-contrast, Monospaced, "Heads-up Display"
 */

export const StatusColors = {
    // Semantic States
    HEALTHY: { bg: '#0F2E1B', text: '#4ADE80', border: '#14532D' }, // Neon Green on Dark
    WARNING: { bg: '#2E1C0F', text: '#FBBF24', border: '#78350F' }, // Amber on Dark
    CRITICAL: { bg: '#2E0F0F', text: '#F87171', border: '#7F1D1D' }, // Red on Dark
    NEUTRAL: { bg: '#09090B', text: '#9CA3AF', border: '#27272A' }, // Zinc

    // UI Elements
    PANEL_BG: '#09090B',
    PANEL_BORDER: '#27272A',
    HEADER_BG: '#18181B',
} as const;

export const StatusTypos = {
    MONO: 'font-mono tracking-tight',
    HEADER: 'uppercase tracking-widest text-[10px] font-bold text-muted-foreground',
    VALUE: 'font-mono font-bold text-sm',
} as const;
