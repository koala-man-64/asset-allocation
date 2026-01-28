#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

import uvicorn

from core.logging_config import configure_logging
from core.config import settings


def main():
    parser = argparse.ArgumentParser(description="Asset Allocation Command Line Interface")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Serve command
    serve_parser = subparsers.add_parser("serve", help="Start the FastAPI server")
    serve_parser.add_argument("--host", default="0.0.0.0", help="Host to bind the server to")
    serve_parser.add_argument("--port", type=int, default=settings.API_PORT, help="Port to bind the server to")
    serve_parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    serve_parser.add_argument("--ui-dir", help="Path to UI dist directory (default: ui/dist)")

    # Task command (placeholder for now)
    task_parser = subparsers.add_parser("task", help="Execute a background task")
    task_parser.add_argument("--name", required=True, help="Name of the task to execute")

    args = parser.parse_args()

    os.environ.setdefault("LOG_LEVEL", settings.log_level)
    os.environ.setdefault("LOG_FORMAT", "TEXT")
    configure_logging()

    if args.command == "serve":
        # Configure UI serving
        ui_dir = args.ui_dir
        if not ui_dir:
            # Default to ui/dist relative to this script
            script_dir = Path(__file__).parent.absolute()
            ui_dir = script_dir / "ui" / "dist"
        else:
            ui_dir = Path(ui_dir).absolute()

        if ui_dir.exists() and ui_dir.is_dir():
            print(f"Serving UI from: {ui_dir}")
            os.environ["BACKTEST_UI_DIST_DIR"] = str(ui_dir)
        else:
            print(f"UI directory not found at: {ui_dir}")
            print("To serve UI: cd ui && npm run build")

        print(f"Starting server on {args.host}:{args.port}...")
        uvicorn.run(
            "api.service.app:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
    elif args.command == "task":
        print(f"Executing task: {args.name}")
        # Logic to route to tasks module
        print("Task execution logic not yet fully implemented.")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
