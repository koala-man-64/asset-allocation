import type { Drawdown, StrategyRun } from '@/types/strategy';

export function getTopDrawdowns(strategy: StrategyRun): Drawdown[] {
  const drawdowns: Drawdown[] = [];
  let inDrawdown = false;
  let ddStart = '';
  let ddTrough = '';
  let ddDepth = 0;
  let troughValue = 0;

  strategy.drawdownCurve.forEach((point, idx) => {
    if (!inDrawdown && point.value < 0) {
      inDrawdown = true;
      ddStart = point.date;
      ddTrough = point.date;
      ddDepth = point.value;
      troughValue = point.value;
      return;
    }

    if (!inDrawdown) return;

    if (point.value < troughValue) {
      ddTrough = point.date;
      ddDepth = point.value;
      troughValue = point.value;
    }

    if (point.value >= 0) {
      const startIdx = strategy.drawdownCurve.findIndex((p) => p.date === ddStart);
      const troughIdx = strategy.drawdownCurve.findIndex((p) => p.date === ddTrough);
      const duration = troughIdx - startIdx;
      const recovery = idx - troughIdx;

      drawdowns.push({
        startDate: ddStart,
        troughDate: ddTrough,
        endDate: point.date,
        depth: ddDepth,
        duration,
        recovery,
      });

      inDrawdown = false;
    }
  });

  if (inDrawdown) {
    const startIdx = strategy.drawdownCurve.findIndex((p) => p.date === ddStart);
    const troughIdx = strategy.drawdownCurve.findIndex((p) => p.date === ddTrough);
    const duration = troughIdx - startIdx;

    drawdowns.push({
      startDate: ddStart,
      troughDate: ddTrough,
      depth: ddDepth,
      duration,
    });
  }

  return drawdowns.sort((a, b) => a.depth - b.depth).slice(0, 5);
}

