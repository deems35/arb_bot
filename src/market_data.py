import asyncio


class MarketData:
    def __init__(self):
        self._data = {}
        self._locks = {}

    def _lock_for(self, pair):
        if pair not in self._locks:
            self._locks[pair] = asyncio.Lock()
        return self._locks[pair]

    async def update(self, pair, side, bid, ask):
        async with self._lock_for(pair):
            self._data[pair] = {'bid': float(bid), 'ask': float(ask)}

    async def get_l1(self, pair):
        async with self._lock_for(pair):
            return self._data.get(pair, None)
