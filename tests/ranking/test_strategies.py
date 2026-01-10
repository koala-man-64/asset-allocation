"""
Unit tests for ranking strategies.
"""
import unittest
from datetime import date
import pandas as pd
import numpy as np

from scripts.ranking.strategies import MomentumStrategy, ValueStrategy

class TestStrategies(unittest.TestCase):
    def setUp(self):
        self.ranking_date = date(2023, 1, 1)
        self.data = pd.DataFrame({
            'symbol': ['A', 'B', 'C', 'D'],
            'return_60d': [0.1, 0.2, -0.05, float('nan')],
            'pe_ratio': [20.0, 10.0, 50.0, -5.0] # D has negative PE often invalid or ignored by simple logic
        })

    def test_momentum_strategy(self):
        strat = MomentumStrategy()
        results = strat.rank(self.data, self.ranking_date)
        
        # Expecting A, B, C (D dropped due to NaN)
        # Order: B (0.2), A (0.1), C (-0.05)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].symbol, 'B')
        self.assertEqual(results[0].rank, 1)
        self.assertEqual(results[1].symbol, 'A')
        self.assertEqual(results[2].symbol, 'C')
        self.assertEqual(results[0].strategy, strat.name)

    def test_value_strategy(self):
        strat = ValueStrategy()
        results = strat.rank(self.data, self.ranking_date)
        
        # Expecting A, B, C. D ignored because negative PE in my mock logic or passed?
        # ValueStrategy code: valid_data = data[(data['pe_ratio'] > 0)]
        # So D (-5.0) should be dropped.
        
        # Order asc: B(10), A(20), C(50)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].symbol, 'B')
        self.assertEqual(results[0].rank, 1)
        self.assertEqual(results[1].symbol, 'A')
        self.assertEqual(results[2].symbol, 'C')

    def test_missing_columns(self):
        strat = MomentumStrategy()
        bad_data = pd.DataFrame({'symbol': ['A'], 'foo': [1]})
        results = strat.rank(bad_data, self.ranking_date)
        self.assertEqual(len(results), 0)
