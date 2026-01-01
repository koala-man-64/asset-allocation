
import sys
import logging
from scripts.common import core as mdc
import os

def main():
    print("Testing logging separation...")
    # Trigger configuration
    mdc.write_line("Init logging")
    
    root_logger = logging.getLogger()
    handlers = root_logger.handlers
    print(f"Handlers: {handlers}")
    
    has_stdout = any(isinstance(h, logging.StreamHandler) and h.stream == sys.stdout for h in handlers)
    has_stderr = any(isinstance(h, logging.StreamHandler) and h.stream == sys.stderr for h in handlers)
    
    with open("verification_result.txt", "w") as f:
        if has_stdout and has_stderr:
            f.write("SUCCESS: Both stdout and stderr handlers detected.")
            print("SUCCESS")
        else:
            f.write(f"FAILURE: Handlers missing. Stdout: {has_stdout}, Stderr: {has_stderr}")
            print("FAILURE")
    
if __name__ == "__main__":
    main()
