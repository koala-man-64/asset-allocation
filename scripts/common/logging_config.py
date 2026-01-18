import logging
import json
import sys
import os
from datetime import datetime
from typing import Any, Dict

# Standard Python Log Levels
# CRITICAL = 50
# ERROR = 40
# WARNING = 30
# INFO = 20
# DEBUG = 10
# NOTSET = 0

class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings for structured logging.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "line": record.lineno,
            "logger": record.name
        }
        
        # Merge extra field if available
        if hasattr(record, 'context') and isinstance(record.context, dict): # type: ignore
            log_record.update(record.context) # type: ignore
            
        # Exception handling
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_record)

def configure_logging(name: str = "root") -> logging.Logger:
    """
    Configures the root logger based on environment variables.
    ENV: LOG_FORMAT (JSON | TEXT) - Defaults to TEXT if missing (Safe default)
    ENV: LOG_LEVEL (DEBUG | INFO | WARNING | ERROR) - Defaults to INFO
    """
    logger = logging.getLogger()
    
    # idempotent configuration
    if logger.handlers:
        return logger
        
    log_format = os.environ.get("LOG_FORMAT", "TEXT").upper()
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    
    level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(level)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    if log_format == "JSON":
        handler.setFormatter(JsonFormatter())
    else:
        # Standard readable format
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(module)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
    logger.addHandler(handler)
    
    # Silence Azure SDK spam
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    return logger
