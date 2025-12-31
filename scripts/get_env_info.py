
import os
from dotenv import load_dotenv

load_dotenv()
print(f"ACCOUNT_NAME={os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}")
