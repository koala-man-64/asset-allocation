import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta

def write_line(msg):
    '''
    Print a line to the console w/ a timestamp
    Parameters:
        str:
    '''
    ct = datetime.now()
    ct = ct.strftime('%Y-%m-%d %H:%M:%S')
    print('{}: {}'.format(ct, msg))

class Trade:
    def __init__(self, date, action, symbol, price, quantity):
        self.Date = date
        self.Action = action
        self.Symbol = symbol
        self.Price = price
        self.Quantity = quantity
        self.ProfitLoss = 0.0
        
    def __repr__(self):
        return f'Date: {self.Date}, Action: {self.Action}, Symbol: {self.Symbol}, Price: {self.Price}, Quantity: {self.Quantity}, ProfitLoss: {self.ProfitLoss}'

class Position:
    def __init__(self, symbol, price, quantity, date):
        self.Symbol = symbol
        self.Quantity = quantity
        self.Price = price
        self.AvgPrice = price
        self.MarketValue = float(round(self.Quantity * self.Price,2))
        self.CostBasis = float(round(self.Quantity * self.AvgPrice,2))
        self.ProfitLoss = float(round(self.MarketValue - self.CostBasis,2))
        self.DateOpened = date
        self.DateClosed = None
        self.LastUpdated = date
    
    def BuyShares(self, buy_quantity, date):
        # build trade object
        t = Trade(date, 'Buy', self.Symbol, self.Price, buy_quantity)
        
        # update quantity and marketvalue
        old_quantity = self.Quantity
        old_avg_price = self.AvgPrice
        self.Quantity += t.Quantity
        self.MarketValue = self.Quantity * self.Price
        
        # calculate and update new cost basis
        self.CostBasis = round(((old_quantity * old_avg_price) + (buy_quantity * self.Price)), 2)
        
        # calculate and update new avg price
        self.AvgPrice = round(self.CostBasis / self.Quantity, 2)
        
        # update lastupdated
        self.LastUpdated = date
        
        return t
    
    def SellShares(self, buy_quantity, date):
        # build trade object
        t = Trade(date, 'Sell', self.Symbol, self.Price, buy_quantity)
        
        # update quantity and marketvalue
        old_quantity = self.Quantity
        old_avg_price = self.AvgPrice
        self.Quantity -= t.Quantity
        self.MarketValue = self.Quantity * self.Price
        
        # calculate and update new cost basis
        self.CostBasis = round(((old_quantity * old_avg_price) - (t.Quantity * self.Price)), 2)
        
        # calculate and update new avg price
        self.AvgPrice = round(self.CostBasis / self.Quantity, 2)
        
        # update lastupdated
        self.LastUpdated = date
        
        return t
    
    def __repr__(self):
        return f'Symbol: {self.Symbol}, Quantity: {self.Quantity}, Price: {self.Price}, MarketValue: {self.MarketValue}, CostBasis: {self.CostBasis}, ProfitLoss: {self.ProfitLoss}'

class Portfolio:
    def __init__(self):
        self.OpenPositions = {} # symbol index
        self.PositionsHistory = {} # date-symbol index
        self.CashBalanceHistory = {} # date index
        self.EquityBalanceHistory = {} # date index
        self.PortfolioBalanceHistory = {} # date index
        self.BenchmarkBalanceHistory = {}
        
    def UpdatePortfolioPrices(self, prices, date, col):
        equity_value = 0
        for pos_index, pos in enumerate(self.OpenPositions):
            position = self.OpenPositions[pos]
            if len(prices[prices['Symbol'] == position.Symbol][col]) > 0:
                position.Price = prices[prices['Symbol'] == position.Symbol][col].values[0]
                position.MarketValue = round(position.Price * position.Quantity, 2)
                position.ProfitLoss = round(position.MarketValue - position.CostBasis, 2)
            position.LastUpdated = date  
            self.OpenPositions[pos] = position
            equity_value += position.MarketValue
            
        self.UpdateEquityBalance(date, equity_value)
        if date not in self.CashBalanceHistory.keys():
            self.UpdateCashBalance(date, self.CurrentCashBalance())  
    
    
    
    def ClosePosition(self, symbol, price, date):
        # lookup open position based on symbol
        pos = self.OpenPositions[symbol]
        
        # if position isn't found, return None
        if pos is None:
            return None
        # else close out position
        else:        
            # update position object with new data
            pos.Price = price
            pos.MarketValue = round(pos.Price * pos.Quantity, 2)
            pos.ProfitLoss = round(pos.MarketValue - pos.CostBasis, 2)
            pos.LastUpdated = date
            pos.DateClosed = date
            
            # remove from open positions
            del self.OpenPositions[symbol]
            
            # update positions history
            self.PositionsHistory[pos.Symbol + '-' + pos.DateOpened.strftime("%Y-%m-%d")] = pos # add to positions history
            
            # update cashbalance history
            if date in self.CashBalanceHistory:
                self.UpdateCashBalance(date, self.CurrentCashBalance() + pos.MarketValue) # add from cash balance  
            
            # update equitybalance history
            if date in self.EquityBalanceHistory:
                self.UpdateEquityBalance(date, self.CurrentEquityBalance() - pos.MarketValue) # subtract from cash balance
                    
            # update portfoliobalancehistory
            self.UpdatePortfolioBalance(date, self.CurrentCashBalance() + self.CurrentEquityBalance())
            
            write_line(f'{pos.Quantity:,} {pos.Symbol} sold on {pos.DateClosed.strftime("%Y-%m-%d")} @ ${pos.Price:,.2f} for ${pos.MarketValue:,.2f}')
            
        return pos
    
    def UpdateCashBalance(self, date, amount):
        self.CashBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
        
    def UpdateEquityBalance(self, date, amount):
        self.EquityBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
        
    def UpdatePortfolioBalance(self, date, amount):
        self.PortfolioBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
    
    def UpdateBenchmarkBalance(self, date, amount):
        self.BenchmarkBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
    
    def CurrentCashBalance(self):
        # check cash balance
        latest_date = max(self.CashBalanceHistory.keys())
        latest_cash_balance = self.CashBalanceHistory[latest_date]
        return latest_cash_balance
    
    def CurrentEquityBalance(self):
        # check cash balance
        latest_date = max(self.EquityBalanceHistory.keys())
        latest_equity_balance = self.EquityBalanceHistory[latest_date]
        return latest_equity_balance
    
    def CurrentBenchmarkBalance(self):
        # check cash balance
        latest_date = max(self.BenchmarkBalanceHistory.keys())
        latest_equity_balance = self.BenchmarkBalanceHistory[latest_date]
        return latest_equity_balance
    
    def CurrentPortfolioBalance(self):
        return self.CurrentCashBalance() + self.CurrentEquityBalance()
    
    def OpenPosition(self, symbol, price, quantity, date):
        # create position object
        pos = Position(symbol, price, quantity, date)

        # if enough cash, minus cash and add to OpenPositions and PositionsHistory
        if pos.CostBasis <= self.CurrentCashBalance():  
                      
            # update positions history
            self.PositionsHistory[pos.Symbol + '-' + pos.DateOpened.strftime("%Y-%m-%d")] = pos # add to positions history
     
            # update open positions
            self.OpenPositions[symbol] = pos # add to open positions
            
            # update cashbalance history
            if date not in self.CashBalanceHistory:
                self.UpdateCashBalance(date, self.CurrentCashBalance())
            self.UpdateCashBalance(date, self.CashBalanceHistory[date] - pos.CostBasis) # subtract from cash balance
            
            # update equitybalance history
            if date not in self.EquityBalanceHistory:
                self.UpdateEquityBalance(date, self.CurrentEquityBalance())
            self.UpdateEquityBalance(date, self.EquityBalanceHistory[date] + pos.CostBasis) # add to equity balance
            
            # update portfoliobalancehistory
            self.UpdatePortfolioBalance(date, self.CashBalanceHistory[date] + self.EquityBalanceHistory[date])
            
        # else send some kind of error response
        else:
            return None
        
        return pos
    
    def AbsoluteReturn():        
        return 0.0
    
    def TotalReturn():
        return 0.0
    
    def AnnualizedReturn():
        return 0.0
    
    def UnrealizedProfitLoss():
        return 0.0
    
    def MaxDrawdown():
        return 0.0
    
    def Variance():
        return 0.0
    
    def WinLossRatio():
        return 0.0
    
    def AverageLossPercent():
        return 0.0
    
    def TotalTrades():
        return 0
    
    def AverageWinPercent():
        return 0.0

class Strategy:
    def __init__(self):
        self.LookbackBars = 30
        self.RiskFreeTicker = 'SPY'
        self.TopNPerGroup = 5
        self.ReturnThreshold = .02
        self.TopNSectors = 20
        self.TopNFinal = 5
        self.YearRangeThreshold = .5
        self.VolumeThreshold = 100000
        self.PriceThreshold = .5
        self.ReallocateThreshold = 5
        self.StopLossThreshold = .03
        self.TakeProfitThreshold = .12
        self.PositionsToMaintain = 4
    
    def __repr__(self):
        return f'LookbackBars: {self.LookbackBars}, RiskFreeTicker: {self.RiskFreeTicker}, TopNPerGroup: {self.TopNPerGroup}, TopNSectors: {self.TopNSectors}, YearRangeThreshold: {self.YearRangeThreshold}, VolumeThreshold: {self.VolumeThreshold}, PriceThreshold: {self.PriceThreshold}'

class BacktestResult:
    def __init__(self):
        self.StartDate = datetime.today().date() - timedelta(days=30)
        self.EndDate = datetime.today().date()
        self.MetricsSnapshots = pd.DataFrame()
        self.Portfolio = Portfolio()
        self.Strategy = Strategy()
        self.SortinoRatio = 0.0
        self.AnnualizedRatio = 0.0
        self.MaxDrawdown = 0.0
        self.Variance = 0.0
        self.TradeHistory = []
        self.ID = uuid.uuid4()
        
    # def __repr__(self):
    #     return f'LookbackBars: {self.LookbackBars}, RiskFreeTicker: {self.RiskFreeTicker}, TopNPerGroup: {self.TopNPerGroup}, TopNSectors: {self.TopNSectors}, YearRangeThreshold: {self.YearRangeThreshold}, VolumeThreshold: {self.VolumeThreshold}, PriceThreshold: {self.PriceThreshold}'

class MetricsSnapshot:
    def __init__(self):
        self.Date = datetime.today().date()
        self.OverallReturn = 0.0
        self.AnnualizedReturn = 0.0
        self.SortinoRatio = 0.0
        self.SharpeRatio = 0.0
        self.InformationRatio = 0.0
        self.UnrealizedProfitLoss = 0.0
        self.MaxDrawdown = 0.0
        self.Variance = 0.0
        self.WinLossRatio = 0.0
        self.AverageLossPercent = 0.0
        self.AverageWinPercent = 0.0
        self.CashBalance = 0.0
        self.AccountBalance = 0.0
        self.PortfolioValue = 0.0
        
    def __repr__(self):
        return f'Date: {self.Date}, AnnualizedReturn: {self.AnnualizedReturn}, SortinoRatio: {self.SortinoRatio}, AccountBalance: {self.AccountBalances}'
