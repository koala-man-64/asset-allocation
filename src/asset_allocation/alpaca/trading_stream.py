import logging
import json
from typing import AsyncIterator, Optional, List

from asset_allocation.alpaca.config import AlpacaConfig
from asset_allocation.alpaca.models import TradeUpdateEvent, AlpacaOrder
from asset_allocation.alpaca.transport_ws import AlpacaWsTransport

logger = logging.getLogger(__name__)

class AlpacaTradingStream:
    def __init__(self, config: AlpacaConfig):
        self._config = config
        self._transport = AlpacaWsTransport(config)

    async def connect(self):
        await self._transport.connect()

    async def listen_trade_updates(self) -> AsyncIterator[TradeUpdateEvent]:
        # Subscribe to trade_updates
        await self._transport.subscribe(["trade_updates"])
        
        async for msg in self._transport.listen():
            stream = msg.get("stream")
            if stream != "trade_updates":
                continue
            
            data = msg.get("data")
            if not data:
                continue

            event_type = data.get("event")
            order_data = data.get("order")
            
            if not order_data:
                continue

            # Convert to internal model
            try:
                # Alpaca stream order object is slightly different or subset, 
                # but usually compatible with the order model if we are careful.
                # We might need to fetch the fresh order from REST if critical fields are missing,
                # but for speed we use what we have.
                # Note: stream data has different keys sometimes? 
                # According to docs, it sends an 'order' object similar to REST.
                order = AlpacaOrder.from_api_dict(order_data)
                
                # Extract execution info
                price = None
                qty = None
                if data.get("price"):
                    price = float(data["price"])
                if data.get("qty"):
                    qty = float(data["qty"])
                
                # Timestamp
                ts_str = data.get("timestamp") or data.get("at") # 'at' sometimes used in events
                if ts_str:
                     # Remove nanoseconds if present 2023-01-01T...
                     # Python's fromisoformat has issues with variable sub-seconds before 3.11 sometimes,
                     # but we assume standard format or handle it.
                     # Alpaca often sends Z.
                     from datetime import datetime
                     ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                else:
                    ts = order.updated_at

                yield TradeUpdateEvent(
                    event=event_type,
                    price=price,
                    qty=qty,
                    timestamp=ts,
                    order=order,
                    execution_id=data.get("execution_id"),
                    position_qty=float(data["position_qty"]) if "position_qty" in data else None
                )

            except Exception as e:
                logger.error(f"Failed to parse trade update: {e} | data: {data}")
                continue

    async def close(self):
        await self._transport.close()
