
import json
import pandas as pd
from pathlib import Path

def load_run(run_id):
    base_dir = Path("backtest_results") / run_id
    summary = json.loads((base_dir / "summary.json").read_text())
    daily = pd.read_csv(base_dir / "daily_metrics.csv")
    
    total_commission = daily["commission"].sum()
    total_slippage = daily["slippage_cost"].sum()
    avg_turnover = daily["turnover"].mean()
    
    return {
        "Annualized Return": f"{summary['annualized_return']:.2%}",
        "Annualized Volatility": f"{summary['annualized_volatility']:.2%}",
        "Sharpe Ratio": f"{summary['sharpe_ratio']:.4f}",
        "Max Drawdown": f"{summary['max_drawdown']:.2%}",
        "Total Return": f"{summary['total_return']:.2%}",
        "Final Equity": f"${summary['final_equity']:,.2f}",
        "Trade Count": summary['trades'],
        "Total Commission": f"${total_commission:,.2f}",
        "Total Slippage": f"${total_slippage:,.2f}",
        "Avg Daily Turnover": f"{avg_turnover:.2%}"
    }

daily = load_run("DAILY-REBAL-2024")
monthly = load_run("MONTHLY-REBAL-2024")

metrics = daily.keys()

print(f"| Metric | Daily Rebalancing | Monthly Rebalancing |")
print(f"| :--- | :--- | :--- |")
for m in metrics:
    print(f"| **{m}** | {daily[m]} | {monthly[m]} |")
