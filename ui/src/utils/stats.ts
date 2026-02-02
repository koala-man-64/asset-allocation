export function mean(values: number[]): number | null {
  if (!values.length) return null;
  const total = values.reduce((sum, v) => sum + v, 0);
  return total / values.length;
}

export function variance(values: number[]): number | null {
  if (values.length < 2) return null;
  const m = mean(values);
  if (m === null) return null;
  const ss = values.reduce((sum, v) => sum + (v - m) * (v - m), 0);
  return ss / (values.length - 1);
}

export function stddev(values: number[]): number | null {
  const v = variance(values);
  if (v === null) return null;
  return Math.sqrt(v);
}

export function correlation(x: number[], y: number[]): number | null {
  if (x.length !== y.length || x.length < 2) return null;
  const mx = mean(x);
  const my = mean(y);
  if (mx === null || my === null) return null;

  let num = 0;
  let ssx = 0;
  let ssy = 0;
  for (let i = 0; i < x.length; i++) {
    const dx = x[i] - mx;
    const dy = y[i] - my;
    num += dx * dy;
    ssx += dx * dx;
    ssy += dy * dy;
  }
  const denom = Math.sqrt(ssx * ssy);
  if (!denom) return null;
  return num / denom;
}

export function skewness(values: number[]): number | null {
  if (values.length < 3) return null;
  const m = mean(values);
  const sd = stddev(values);
  if (m === null || sd === null || sd === 0) return null;
  const n = values.length;
  const m3 = values.reduce((sum, v) => sum + Math.pow((v - m) / sd, 3), 0) / n;
  return m3;
}

export function kurtosis(values: number[]): number | null {
  if (values.length < 4) return null;
  const m = mean(values);
  const sd = stddev(values);
  if (m === null || sd === null || sd === 0) return null;
  const n = values.length;
  const m4 = values.reduce((sum, v) => sum + Math.pow((v - m) / sd, 4), 0) / n;
  return m4;
}

export interface MonthlyReturn {
  year: number;
  month: number;
  return: number; // percent
}

export function computeMonthlyReturns(
  points: Array<{ date: string; dailyReturn: number }>
): MonthlyReturn[] {
  const byMonth = new Map<string, { year: number; month: number; product: number }>();
  for (const point of points) {
    const date = new Date(point.date);
    if (Number.isNaN(date.getTime())) continue;
    const year = date.getFullYear();
    const month = date.getMonth() + 1;
    const key = `${year}-${month}`;
    const existing = byMonth.get(key);
    const product = (existing?.product ?? 1) * (1 + point.dailyReturn);
    byMonth.set(key, { year, month, product });
  }

  const out: MonthlyReturn[] = [];
  for (const { year, month, product } of byMonth.values()) {
    out.push({ year, month, return: (product - 1) * 100 });
  }
  out.sort((a, b) => a.year - b.year || a.month - b.month);
  return out;
}

export interface DrawdownPeriod {
  startDate: string;
  troughDate: string;
  endDate?: string;
  depth: number; // percent (negative)
  duration: number; // days (index delta)
  recovery?: number; // days (index delta)
}

export function computeTopDrawdowns(
  drawdownPctSeries: Array<{ date: string; value: number }>
): DrawdownPeriod[] {
  const drawdowns: DrawdownPeriod[] = [];
  let inDrawdown = false;
  let startDate = '';
  let troughDate = '';
  let troughValue = 0;

  for (let idx = 0; idx < drawdownPctSeries.length; idx++) {
    const point = drawdownPctSeries[idx];
    if (!inDrawdown && point.value < 0) {
      inDrawdown = true;
      startDate = point.date;
      troughDate = point.date;
      troughValue = point.value;
      continue;
    }

    if (!inDrawdown) continue;

    if (point.value < troughValue) {
      troughValue = point.value;
      troughDate = point.date;
    }

    if (point.value >= 0) {
      const startIdx = drawdownPctSeries.findIndex((p) => p.date === startDate);
      const troughIdx = drawdownPctSeries.findIndex((p) => p.date === troughDate);
      const duration = Math.max(0, troughIdx - startIdx);
      const recovery = Math.max(0, idx - troughIdx);
      drawdowns.push({
        startDate,
        troughDate,
        endDate: point.date,
        depth: troughValue,
        duration,
        recovery
      });
      inDrawdown = false;
    }
  }

  if (inDrawdown) {
    const startIdx = drawdownPctSeries.findIndex((p) => p.date === startDate);
    const troughIdx = drawdownPctSeries.findIndex((p) => p.date === troughDate);
    const duration = Math.max(0, troughIdx - startIdx);
    drawdowns.push({
      startDate,
      troughDate,
      depth: troughValue,
      duration
    });
  }

  return drawdowns.sort((a, b) => a.depth - b.depth).slice(0, 5);
}
