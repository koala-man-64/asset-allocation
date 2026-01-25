
import os
import sys
import yaml
import json
import argparse
from typing import Optional
from dotenv import load_dotenv

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load env vars
load_dotenv()

from core.strategy_repository import StrategyRepository

def main():
    parser = argparse.ArgumentParser(description="Manage strategies in Postgres.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # List
    parser_list = subparsers.add_parser("list", help="List all strategies")

    # Upload
    parser_upload = subparsers.add_parser("upload", help="Upload a strategy from a YAML/JSON file")
    parser_upload.add_argument("file", help="Path to strategy config file")
    parser_upload.add_argument("--name", help="Override strategy name")
    parser_upload.add_argument("--type", default="configured", help="Strategy type (default: configured)")
    parser_upload.add_argument("--description", default="", help="Description")

    # Get
    parser_get = subparsers.add_parser("get", help="Get strategy config by name")
    parser_get.add_argument("name", help="Strategy name")

    args = parser.parse_args()
    repo = StrategyRepository()

    if args.command == "list":
        strategies = repo.list_strategies()
        print(f"Found {len(strategies)} strategies:")
        print(f"{'Name':<30} | {'Type':<15} | {'Updated At'}")
        print("-" * 70)
        for s in strategies:
            print(f"{s['name']:<30} | {s['type']:<15} | {s['updated_at']}")

    elif args.command == "upload":
        # Read file
        with open(args.file, 'r') as f:
            if args.file.endswith('.yaml') or args.file.endswith('.yml'):
                content = yaml.safe_load(f)
            else:
                content = json.load(f)

        # Extract name if not provided
        # If the file is a full backtest config, look for 'strategy' block
        # If it's just a strategy block, use it directly
        
        config_payload = content
        
        # Heuristic: If it has 'strategy' key and 'universe' key at top level, it might be a backtest config
        if "strategy" in content and isinstance(content["strategy"], dict):
             print("Detected backtest config structure. Extracting 'strategy' block.")
             config_payload = content["strategy"]

        # Default name
        name = args.name or config_payload.get("name") or "Unnamed_Strategy"
        
        repo.save_strategy(name, config_payload, args.type, args.description)
        print(f"Strategy '{name}' saved successfully.")

    elif args.command == "get":
        config = repo.get_strategy_config(args.name)
        if config:
            print(json.dumps(config, indent=2))
        else:
            print(f"Strategy '{args.name}' not found.")
            sys.exit(1)

if __name__ == "__main__":
    main()
