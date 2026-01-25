/**
 * Industrial Utility Design Tokens
 * Aesthetic: High-contrast, Monospaced, "Heads-up Display"
 */

export const StatusColors = {
    // Semantic States
    HEALTHY: { bg: '#EAF7F0', text: '#187A3E', border: '#BFE6CD' }, // Soft green on light
    WARNING: { bg: '#FFF7DF', text: '#9A6B00', border: '#F0D490' }, // Warm amber on light
    CRITICAL: { bg: '#FDECEC', text: '#B3261E', border: '#F5B5B0' }, // Soft red on light
    NEUTRAL: { bg: '#F5F5F7', text: '#52525B', border: '#E4E4E7' }, // Neutral

    // UI Elements
    PANEL_BG: '#F8FAFC',
    PANEL_BORDER: '#E2E8F0',
    HEADER_BG: '#F1F5F9',
} as const;

export const StatusTypos = {
    MONO: 'font-mono tracking-tight',
    HEADER: 'uppercase tracking-widest text-[10px] font-bold text-muted-foreground',
    VALUE: 'font-mono font-bold text-sm',
} as const;
