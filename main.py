#!/usr/bin/env python3
import argparse

import uvicorn

from core.logging import setup_logging
from core.config import settings


def main():
    parser = argparse.ArgumentParser(description="Asset Allocation Command Line Interface")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Serve command
    serve_parser = subparsers.add_parser("serve", help="Start the FastAPI server")
    serve_parser.add_argument("--host", default="0.0.0.0", help="Host to bind the server to")
    serve_parser.add_argument("--port", type=int, default=8000, help="Port to bind the server to")
    serve_parser.add_argument("--reload", action="store_true", help="Enable auto-reload")

    # Task command (placeholder for now)
    task_parser = subparsers.add_parser("task", help="Execute a background task")
    task_parser.add_argument("--name", required=True, help="Name of the task to execute")

    args = parser.parse_args()

    setup_logging(level=settings.log_level)

    if args.command == "serve":
        print(f"Starting server on {args.host}:{args.port}...")
        uvicorn.run(
            "backtest.service.app:create_app",
            host=args.host,
            port=args.port,
            reload=args.reload,
            factory=True,
        )
    elif args.command == "task":
        print(f"Executing task: {args.name}")
        # Logic to route to tasks module
        print("Task execution logic not yet fully implemented.")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
