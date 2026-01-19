import os

import uvicorn

def main_loop() -> None:
    host = os.environ.get("UI_HOST", "0.0.0.0")
    port = int(os.environ.get("UI_PORT", "8001"))
    uvicorn.run("asset_allocation.ui.web:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main_loop()
