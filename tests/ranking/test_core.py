"""
Unit tests for ranking core module.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
from datetime import date
import pandas as pd
import json

from tasks.ranking.core import RankingResult, save_rankings, get_rankings

class TestRankingResult(unittest.TestCase):
    def test_to_dict(self):
        rr = RankingResult(
            date=date(2023, 1, 1),
            strategy="TestStrat",
            symbol="AAPL",
            rank=1,
            score=100.5,
            meta={"foo": "bar"}
        )
        d = rr.to_dict()
        self.assertEqual(d['date'], "2023-01-01")
        self.assertEqual(d['strategy'], "TestStrat")
        self.assertEqual(d['meta'], '{"foo": "bar"}')

class TestCoreFunctions(unittest.TestCase):
    @patch('tasks.ranking.core.store_delta')
    @patch('tasks.ranking.core.cfg')
    def test_save_rankings(self, mock_cfg, mock_store_delta):
        mock_cfg.AZURE_CONTAINER_RANKING = 'test-container'
        os.environ["AZURE_CONTAINER_RANKING"] = "test-container"
        
        rankings = [
            RankingResult(date=date(2023, 1, 1), strategy="S", symbol="A", rank=1, score=10.0),
            RankingResult(date=date(2023, 1, 1), strategy="S", symbol="B", rank=2, score=5.0)
        ]
        
        save_rankings(rankings)
        
        mock_store_delta.assert_called_once()
        call_args = mock_store_delta.call_args
        df_arg = call_args[0][0]
        
        self.assertIsInstance(df_arg, pd.DataFrame)
        self.assertEqual(len(df_arg), 2)
        self.assertEqual(call_args[1]['container'], 'test-container')
        self.assertEqual(call_args[1]['partition_by'], ['strategy', 'date'])

    @patch('tasks.ranking.core.load_delta')
    def test_get_rankings(self, mock_load_delta):
        os.environ["AZURE_CONTAINER_RANKING"] = "test-container"
        # Mock dataframe return
        data = {
            'strategy': ['A', 'A', 'B'],
            'date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-01')],
            'symbol': ['S1', 'S2', 'S3'],
            'rank': [2, 1, 1],
            'score': [10.0, 20.0, 100.0]
        }
        df = pd.DataFrame(data)
        mock_load_delta.return_value = df
        
        # Test filter
        result = get_rankings("A", date(2023, 1, 1))
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['symbol'], 'S2') # Rank 1 should be first
