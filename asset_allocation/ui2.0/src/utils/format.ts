export function toPercent(value: number | null | undefined): number | null {
  if (value === null || value === undefined || Number.isNaN(value)) return null;
  return value * 100;
}

export function formatNumber(value: number | null | undefined, digits: number = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return value.toFixed(digits);
}

export function formatPercentDecimal(value: number | null | undefined, digits: number = 1): string {
  const pct = toPercent(value);
  if (pct === null) return '—';
  return `${pct.toFixed(digits)}%`;
}

export function formatPercent(value: number | null | undefined, digits: number = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return `${value.toFixed(digits)}%`;
}

export function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(value);
}

