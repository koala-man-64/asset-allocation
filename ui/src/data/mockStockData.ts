import { Stock } from '@/types/stock';

const EXCHANGES = ['NASDAQ', 'NYSE', 'AMEX'];

function generatePriceHistory(startPrice: number, days: number = 252) {
  const history = [];
  let currentPrice = startPrice;
  const volatility = 0.02;

  for (let i = 0; i < days; i++) {
    const change = 1 + (Math.random() * volatility * 2 - volatility);
    const open = currentPrice;
    const close = currentPrice * change;
    const high = Math.max(open, close) * (1 + Math.random() * 0.01);
    const low = Math.min(open, close) * (1 - Math.random() * 0.01);

    currentPrice = close;

    const date = new Date();
    date.setDate(date.getDate() - (days - i));

    history.push({
      date: date.toISOString().split('T')[0],
      open,
      high,
      low,
      close,
      price: close,
      volume: Math.floor(Math.random() * 1000000 + 500000)
    });
  }
  return history;
}

const SYMBOLS = [
  { s: 'AAPL', n: 'Apple Inc.', sec: 'Technology' },
  { s: 'MSFT', n: 'Microsoft Corp.', sec: 'Technology' },
  { s: 'GOOGL', n: 'Alphabet Inc.', sec: 'Technology' },
  { s: 'AMZN', n: 'Amazon.com Inc.', sec: 'Consumer Discretionary' },
  { s: 'NVDA', n: 'NVIDIA Corp.', sec: 'Technology' },
  { s: 'BRK.B', n: 'Berkshire Hathaway', sec: 'Financials' },
  { s: 'META', n: 'Meta Platforms', sec: 'Technology' },
  { s: 'TSLA', n: 'Tesla Inc.', sec: 'Consumer Discretionary' },
  { s: 'LLY', n: 'Eli Lilly & Co.', sec: 'Healthcare' },
  { s: 'V', n: 'Visa Inc.', sec: 'Financials' },
  { s: 'JPM', n: 'JPMorgan Chase', sec: 'Financials' },
  { s: 'WMT', n: 'Walmart Inc.', sec: 'Consumer Staples' },
  { s: 'XOM', n: 'Exxon Mobil', sec: 'Energy' },
  { s: 'UNH', n: 'UnitedHealth Group', sec: 'Healthcare' },
  { s: 'MA', n: 'Mastercard Inc.', sec: 'Financials' },
  { s: 'PG', n: 'Procter & Gamble', sec: 'Consumer Staples' },
  { s: 'JNJ', n: 'Johnson & Johnson', sec: 'Healthcare' },
  { s: 'HD', n: 'Home Depot', sec: 'Consumer Discretionary' },
  { s: 'MRK', n: 'Merck & Co.', sec: 'Healthcare' },
  { s: 'COST', n: 'Costco Wholesale', sec: 'Consumer Staples' },
  { s: 'ABBV', n: 'AbbVie Inc.', sec: 'Healthcare' },
  { s: 'AMD', n: 'Advanced Micro Devices', sec: 'Technology' },
  { s: 'ADBE', n: 'Adobe Inc.', sec: 'Technology' },
  { s: 'CRM', n: 'Salesforce Inc.', sec: 'Technology' },
  { s: 'NFLX', n: 'Netflix Inc.', sec: 'Consumer Discretionary' },
  { s: 'BAC', n: 'Bank of America', sec: 'Financials' },
  { s: 'KO', n: 'Coca-Cola Co.', sec: 'Consumer Staples' },
  { s: 'PEP', n: 'PepsiCo Inc.', sec: 'Consumer Staples' },
  { s: 'TMO', n: 'Thermo Fisher Scientific', sec: 'Healthcare' },
  { s: 'LIN', n: 'Linde plc', sec: 'Materials' },
  { s: 'MCD', n: "McDonald's Corp.", sec: 'Consumer Discretionary' },
  { s: 'DIS', n: 'Walt Disney Co.', sec: 'Consumer Discretionary' },
  { s: 'CSCO', n: 'Cisco Systems', sec: 'Technology' },
  { s: 'ACN', n: 'Accenture plc', sec: 'Technology' },
  { s: 'ABT', n: 'Abbott Laboratories', sec: 'Healthcare' },
  { s: 'DHR', n: 'Danaher Corp.', sec: 'Healthcare' },
  { s: 'NKE', n: 'Nike Inc.', sec: 'Consumer Discretionary' },
  { s: 'VZ', n: 'Verizon Communications', sec: 'Utilities' },
  { s: 'NEE', n: 'NextEra Energy', sec: 'Utilities' },
  { s: 'TXN', n: 'Texas Instruments', sec: 'Technology' }
];

export async function getStocks(_dataSource: string): Promise<Stock[]> {
  // Simulate delay
  await new Promise((resolve) => setTimeout(resolve, 800));

  return SYMBOLS.map((item, index) => {
    const price = 50 + Math.random() * 500;
    const history = generatePriceHistory(price);
    const lastPrice = history[history.length - 1].close;
    const startPrice = history[0].open;
    const priceChange = ((lastPrice - startPrice) / startPrice) * 100;

    return {
      id: `stock-${index}`,
      symbol: item.s,
      name: item.n,
      sector: item.sec,
      industry: 'Industry ' + Math.floor(Math.random() * 10),
      marketCap: 10 + Math.random() * 2000,
      price: lastPrice,
      priceChange1Y: priceChange,
      pe: Math.random() > 0.1 ? 10 + Math.random() * 50 : null,
      pb: Math.random() * 10,
      ps: Math.random() * 15,
      pegRatio: Math.random() * 3,
      dividendYield: Math.random() > 0.3 ? Math.random() * 5 : null,
      beta: 0.5 + Math.random() * 1.5,
      volume: 500000 + Math.random() * 10000000,
      avgVolume: 500000 + Math.random() * 10000000,
      high52W: Math.max(...history.map((h) => h.high)),
      low52W: Math.min(...history.map((h) => h.low)),
      roe: Math.random() * 30,
      roa: Math.random() * 15,
      profitMargin: Math.random() * 25,
      revenueGrowth: -5 + Math.random() * 30,
      earningsGrowth: -10 + Math.random() * 40,
      debtToEquity: Math.random() * 2,
      freeCashFlow: Math.random() * 1000,
      exchange: EXCHANGES[Math.floor(Math.random() * EXCHANGES.length)],
      country: 'USA',
      ipoDate: new Date(2000 + Math.floor(Math.random() * 20), 0, 1).toISOString(),
      description: `Description for ${item.n}`,
      employees: Math.floor(Math.random() * 100000),
      website: `https://www.example.com/${item.s}`,
      esgScore: 50 + Math.random() * 50,
      analystRating: ['Buy', 'Hold', 'Sell'][Math.floor(Math.random() * 3)] as
        | 'Buy'
        | 'Hold'
        | 'Sell',
      analystCount: Math.floor(Math.random() * 50),
      volatility52W: 10 + Math.random() * 50,
      rsi14: 30 + Math.random() * 40,
      evToEbitda: 5 + Math.random() * 20,
      highVolatility: Math.random() > 0.8,
      negativeEarnings: Math.random() > 0.9,
      lowLiquidity: Math.random() > 0.9,
      priceHistory: history
    };
  });
}
