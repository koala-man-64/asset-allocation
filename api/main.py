from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

# Import routers
from .endpoints import aliases, data, ranking

app = FastAPI(title="Asset Allocation API", version="1.0.0")

# CORS Configuration
origins = [
    "http://localhost:3000",
    "http://localhost:5173",  # Vite default
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health_check():
    return {"status": "ok", "env": os.environ.get("ASSET_ALLOCATION_ENV")}

# Include Routers
app.include_router(data.router, prefix="/data", tags=["Data"])
app.include_router(ranking.router, prefix="/ranking", tags=["Ranking"])
app.include_router(aliases.router, tags=["Aliases"])
