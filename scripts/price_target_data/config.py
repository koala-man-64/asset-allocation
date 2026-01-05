
from scripts.common.config_shared import *

# Local Specific Requirements
AZURE_CONTAINER_TARGETS = require_env("AZURE_CONTAINER_TARGETS")

# Nasdaq API Key is only needed here
NASDAQ_API_KEY = os.environ.get("NASDAQ_API_KEY") # Optional or Required? User script usually treats it as possibly optional if logic handles it, but let's check scraper usage if we can.
# Checking usage previously implies it was environment-variable based.
# To be safe and strict, let's look at the scraper logic if we were unsure, but for now we follow the pattern.
# However, standardizing:
NASDAQ_API_KEY = os.environ.get("NASDAQ_API_KEY") 
