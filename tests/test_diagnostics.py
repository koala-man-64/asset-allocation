
import os
import logging
from unittest.mock import patch, call
from scripts.common import core as mdc

def test_log_environment_diagnostics(caplog, capsys):
    """
    Verifies that log_environment_diagnostics:
    1. Logs all environment variables.
    2. Masks sensitive keys.
    """
    # Setup test env
    test_env = {
        'NORMAL_VAR': 'visible',
        'TEST_KEY': 'SecretKey123',
        'TEST_PASSWORD': 'SuperSecretPassword',
        'TEST_CONN_STR': 'Endpoint=sb://;SharedAccessKey=abc',
        'TEST_SHORT_TOKEN': '123'
    }
    
    with patch.dict(os.environ, test_env, clear=True):
         with caplog.at_level(logging.INFO):
             mdc.log_environment_diagnostics()
             
    # Analyze logs
    logs = caplog.text
    captured = capsys.readouterr()
    stdout_content = captured.out
    
    # 1. Check Header (Printed to stdout via write_section)
    assert "ENVIRONMENT DIAGNOSTICS" in stdout_content
    
    # 2. Check Normal Var (Logged via logger)
    assert "NORMAL_VAR = visible" in logs
    
    # 3. Check Masking
    # TEST_KEY should be masked (len > 4) -> "Sec...***(12 chars)"
    assert "TEST_KEY = Sec...***(12 chars)" in logs
    
    # TEST_PASSWORD
    assert "TEST_PASSWORD = Sup...***(19 chars)" in logs
    
    # TEST_CONN_STR
    assert "TEST_CONN_STR = End...***(34 chars)" in logs
    
    # TEST_SHORT_TOKEN (len 3) -> "***"
    assert "TEST_SHORT_TOKEN = ***" in logs

    # Ensure raw secret is NOT valid
    assert "SecretKey123" not in logs
    assert "SuperSecretPassword" not in logs
