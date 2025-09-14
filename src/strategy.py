import asyncio
import math
from loguru import logger


class Strategy:
    def __init__(self, cfg, market_data, order_manager, state):
        self.cfg = cfg
        self.market_data = market_data
        self.order_manager = order_manager
        self.state = state
        self.qcache = {}
        self.top_pairs = []
        self._running = True

    async def on_new_scan(self, cache, top):
        self.qcache = cache
        self.top_pairs = top

    async def run(self):
        while self._running:
            try:
                top_set = set(self.top_pairs or [])
                open_list = await self.state.list_open()
                open_set = set(open_list or [])
                pairs_to_check = list(top_set | open_set)

                for pair in pairs_to_check:
                    q_entry = self.qcache.get(pair)
                    if q_entry is None:
                        open_info = await self.state.get_open_info(pair)
                        if open_info and isinstance(open_info, dict):
                            q_vals = open_info.get('q')
                            if q_vals and len(q_vals) >= 3:
                                q_low, q_high, width = q_vals
                                q = (q_low, q_high, width)
                            elif q_vals and len(q_vals) == 2:
                                q_low, q_high = q_vals
                                q = (q_low, q_high, None)
                            else:
                                q = None
                        else:
                            q = None
                    else:
                        q_low, q_high, width = q_entry
                        q = (q_low, q_high, width)

                    if not q:
                        continue

                    q_low, q_high, width = q

                    b = await self.market_data.get_l1((pair[0], pair[1], 'binance'))
                    y = await self.market_data.get_l1((pair[0], pair[1], 'bybit'))
                    if not b or not y:
                        continue

                    try:
                        spread_in = math.log(float(b['ask'])) - math.log(float(y['bid']))
                        has_open = await self.state.has_open(pair)

                        if pair in top_set and spread_in < q_low and not has_open:
                            logger.info('OPEN signal for {p}: spread_in={s} < q_low={q}', p=pair, s=spread_in, q=q_low)
                            res = await self.order_manager.open_pair(pair)
                            if res:
                                await self.state.mark_open(pair, {'orders': res, 'q': (q_low, q_high, width)})

                        spread_out = math.log(float(b['bid'])) - math.log(float(y['ask']))
                        has_open = await self.state.has_open(pair)
                        if has_open and spread_out > q_high:
                            logger.info('CLOSE signal for {p}: spread_out={s} > q_high={q}', p=pair, s=spread_out, q=q_high)
                            res = await self.order_manager.close_pair(pair)
                            if res:
                                await self.state.mark_closed(pair, res)
                                if self.state.closed_count >= self.cfg['strategy']['MAX_CLOSED_TRADES']:
                                    logger.info('MAX_CLOSED_TRADES reached. Stopping')
                                    self._running = False
                                    return
                    except Exception as e:
                        logger.error('Strategy error for {p}: {e}', p=pair, e=e)
            except Exception as main_e:
                logger.exception('Strategy main loop exception: {}', main_e)

            await asyncio.sleep(1)
