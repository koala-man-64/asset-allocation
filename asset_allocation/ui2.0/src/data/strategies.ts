export interface Contribution {
    name: string;
    type: string;
    contribution: number;
}

export interface RollingMetric {
    date: string;
    value: number;
}

export interface RollingMetrics {
    beta: RollingMetric[];
    sharpe: RollingMetric[];
}

export interface Strategy {
    id: string;
    name: string;
    cagr: number;
    annVol: number;
    sharpe: number;
    maxDrawdown: number;
    contributions: Contribution[];
    rollingMetrics: RollingMetrics;
    betaToBenchmark: number;
}

export interface StressEvent {
    name: string;
    date: string;
    strategyReturn: number;
    benchmarkReturn: number;
}

const generateContributions = (): Contribution[] => {
    const types = ["symbol", "sector", "factor"];
    const contributions: Contribution[] = [];

    // Symbols
    ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "BRK.B", "JPM", "V"].forEach(name => {
        contributions.push({ name, type: "symbol", contribution: (Math.random() * 10) - 2 });
    });

    // Sectors
    ["Technology", "Financials", "Healthcare", "Consumer Discretionary", "Industrials"].forEach(name => {
        contributions.push({ name, type: "sector", contribution: (Math.random() * 5) - 1 });
    });

    // Factors
    ["Momentum", "Value", "Size", "Quality", "Low Volatility"].forEach(name => {
        contributions.push({ name, type: "factor", contribution: (Math.random() * 4) - 0.5 });
    });

    return contributions;
};

const generateRollingMetrics = (): RollingMetrics => {
    const rollingBeta = [];
    const rollingSharpe = [];
    for (let i = 0; i < 36; i++) {
        const date = '2023-\${(i % 12) + 1}-01';
        rollingBeta.push({ date, value: 0.8 + Math.random() * 0.4 });
        rollingSharpe.push({ date, value: 0.5 + Math.random() * 1.0 });
    }
    return { beta: rollingBeta, sharpe: rollingSharpe };
};

export const stressEvents: StressEvent[] = [
    { name: "COVID-19 Crash", date: "2020-03-20", strategyReturn: -15.4, benchmarkReturn: -33.8 },
    { name: "Inflation Spike", date: "2022-06-15", strategyReturn: -8.2, benchmarkReturn: -20.5 },
    { name: "Banking Crisis", date: "2023-03-10", strategyReturn: -2.1, benchmarkReturn: -5.4 },
    { name: "Tech Selloff", date: "2018-12-24", strategyReturn: -12.5, benchmarkReturn: -19.8 },
    { name: "Volatility Spike", date: "2024-01-15", strategyReturn: -1.5, benchmarkReturn: -4.2 }
];

export const mockStrategies: Strategy[] = [
    {
        id: "s1",
        name: "Golden Butterfly",
        cagr: 6.5,
        annVol: 7.8,
        sharpe: 0.83,
        maxDrawdown: -11.2,
        contributions: generateContributions(),
        rollingMetrics: generateRollingMetrics(),
        betaToBenchmark: 0.65
    },
    {
        id: "s2",
        name: "Risk Parity Basic",
        cagr: 7.2,
        annVol: 9.1,
        sharpe: 0.79,
        maxDrawdown: -15.4,
        contributions: generateContributions(),
        rollingMetrics: generateRollingMetrics(),
        betaToBenchmark: 0.85
    },
    {
        id: "s3",
        name: "All Weather",
        cagr: 5.8,
        annVol: 6.2,
        sharpe: 0.94,
        maxDrawdown: -9.5,
        contributions: generateContributions(),
        rollingMetrics: generateRollingMetrics(),
        betaToBenchmark: 0.45
    },
    {
        id: "s4",
        name: "Permanent Portfolio",
        cagr: 5.2,
        annVol: 5.5,
        sharpe: 0.95,
        maxDrawdown: -8.9,
        contributions: generateContributions(),
        rollingMetrics: generateRollingMetrics(),
        betaToBenchmark: 0.40
    },
    {
        id: "s5",
        name: "60/40 Benchmark",
        cagr: 8.5,
        annVol: 12.1,
        sharpe: 0.70,
        maxDrawdown: -22.3,
        contributions: generateContributions(),
        rollingMetrics: generateRollingMetrics(),
        betaToBenchmark: 1.00
    }
];
