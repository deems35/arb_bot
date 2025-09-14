import asyncio


class TradeState:
    def __init__(self):
        self.open_positions = {}
        self.closed_count = 0
        self._lock = asyncio.Lock()

    async def has_open(self, pair):
        async with self._lock:
            return pair in self.open_positions

    async def mark_open(self, pair, info):
        async with self._lock:
            self.open_positions[pair] = info

    async def mark_closed(self, pair, info):
        async with self._lock:
            if pair in self.open_positions:
                del self.open_positions[pair]
            self.closed_count += 1

    async def list_open(self):
        async with self._lock:
            return list(self.open_positions.keys())

    async def get_open_info(self, pair):
        async with self._lock:
            return self.open_positions.get(pair)
