import asyncio
import json
import logging
from typing import Set

import asyncpg
from fastapi import WebSocket, WebSocketDisconnect
from api.service.settings import ServiceSettings

logger = logging.getLogger("backtest.realtime")

class BroadcastManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        
        text = json.dumps(message)
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(text)
            except Exception:
                disconnected.append(connection)
        
        for connection in disconnected:
            self.disconnect(connection)

manager = BroadcastManager()

async def listen_to_postgres(settings: ServiceSettings):
    dsn = settings.postgres_dsn
    if not dsn:
        logger.warning("No Postgres DSN configured. Real-time updates disabled.")
        return

    logger.info("Starting Postgres listener on channel 'run_updates'...")
    
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.add_listener("run_updates", _handle_notification)
            logger.info("Listening for database notifications...")
            while True:
                await asyncio.sleep(60) # Keep connection alive
                if conn.is_closed():
                    logger.warning("Postgres connection closed. Reconnecting...")
                    break
        finally:
            if not conn.is_closed():
                await conn.close()
            logger.info("Postgres listener stopped.")

    except Exception as e:
        logger.error(f"Postgres listener error: {e}")
        # Simple backoff restart strategy could be implemented here or in the caller
        await asyncio.sleep(5) 

def _handle_notification(connection, pid, channel, payload):
    logger.debug(f"Received notification: {payload}")
    try:
        data = json.loads(payload)
        asyncio.create_task(manager.broadcast({"type": "RUN_UPDATE", "payload": data}))
    except Exception as e:
        logger.error(f"Failed to process notification payload: {e}")
