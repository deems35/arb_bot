"""
Order manager: handling of market orders on Binance (futures) and Bybit (linear).

Atomic open/close: если сделка на одной бирже открылaсь, а на другой нет,
мы пытаемся компенсировать и логируем/оповещаем.
"""
import asyncio
import math
import time
from loguru import logger
from binance.client import Client as BinanceClient
from pybit.unified_trading import HTTP as BybitClient
from src.utils.math_utils import decimal_floor_to_step
from decimal import Decimal


class ExchangeInfoCache:
    def __init__(self, b_client: BinanceClient, y_client: BybitClient):
        self.b_client = b_client
        self.y_client = y_client
        self._cache = {}
        self._full_binance_info = None
        self._lock = asyncio.Lock()

    async def get(self, bsym: str, ysym: str):
        key = (bsym, ysym)
        async with self._lock:
            if key in self._cache and time.time() - self._cache[key]['ts'] < 300:
                return self._cache[key]['data']
        # fetch info concurrently in threadworkers
        data = await asyncio.gather(
            asyncio.to_thread(self._get_binance_symbol_info, bsym),
            asyncio.to_thread(self._get_bybit_symbol_info, ysym),
        )
        res = {'binance': data[0], 'bybit': data[1]}
        async with self._lock:
            self._cache[key] = {'ts': time.time(), 'data': res}
        return res

    def _load_full_binance_info(self):
        if self._full_binance_info is None:
            info = self.b_client.futures_exchange_info()
            self._full_binance_info = info.get('symbols', [])
        return self._full_binance_info

    def _get_binance_symbol_info(self, symbol):
        symbols = self._load_full_binance_info()
        for s in symbols:
            if s.get('symbol') == symbol:
                fs = {f['filterType']: f for f in s.get('filters', [])}
                lot = fs.get('LOT_SIZE', {})
                min_notional = fs.get('MIN_NOTIONAL', {})
                return {
                    'symbol': symbol,
                    'stepSize': float(lot.get('stepSize', 0)) if lot else 0,
                    'minQty': float(lot.get('minQty', 0)) if lot else 0,
                    'minNotional': float(min_notional.get('minNotional', 0)) if min_notional else 0,
                }
        return {'symbol': symbol, 'stepSize': 0, 'minQty': 0, 'minNotional': 0}

    def _get_bybit_symbol_info(self, symbol):
        try:
            res = self.y_client.query_symbol(symbol=symbol)
            r = res.get('result') or res
            lotSz = None
            minQty = None
            minNotional = None
            if isinstance(r, dict):
                for k in ['list', 'symbols', 'result']:
                    if k in r and isinstance(r[k], (list, tuple)) and len(r[k]) > 0:
                        entry = r[k][0]
                        # Bybit unified formats vary
                        lotSz = entry.get('lot_size_filter') or entry.get('stepSize') or entry.get('tick_size') or entry.get('lotSize')
                        minQty = entry.get('min_qty') or entry.get('minQty') or entry.get('minOrderQty')
                        break
            return {
                'symbol': symbol,
                'stepSize': float(lotSz) if lotSz else 0,
                'minQty': float(minQty) if minQty else 0,
                'minNotional': float(minNotional) if minNotional else 0,
            }
        except Exception as e:
            logger.error('Bybit symbol info error: {e}', e=e)
            return {'symbol': symbol, 'stepSize': 0, 'minQty': 0, 'minNotional': 0}


class OrderManager:
    def __init__(self, cfg, market_data, dry_run=False):
        self.cfg = cfg
        self.market_data = market_data
        self.dry_run = dry_run
        self.b_client = BinanceClient(cfg['api']['binance_api_key'], cfg['api']['binance_api_secret'])
        self.y_client = BybitClient(api_key=cfg['api']['bybit_api_key'], api_secret=cfg['api']['bybit_api_secret'])
        self.exchange_cache = ExchangeInfoCache(self.b_client, self.y_client)
        self._pair_locks = {}

    def _pair_lock(self, pair):
        if pair not in self._pair_locks:
            self._pair_locks[pair] = asyncio.Lock()
        return self._pair_locks[pair]

    async def calc_qty(self, pair):
        bsym, ysym = pair
        bmd = await self.market_data.get_l1((bsym, ysym, 'binance'))
        ymd = await self.market_data.get_l1((bsym, ysym, 'bybit'))
        if not bmd or not ymd:
            logger.warning('Missing L1 for qty calc {}', pair)
            return None
        price_b = (Decimal(str(float(bmd['bid']))) + Decimal(str(float(bmd['ask'])))) / Decimal(2)
        price_y = (Decimal(str(float(ymd['bid']))) + Decimal(str(float(ymd['ask'])))) / Decimal(2)
        usd = Decimal(str(self.cfg['strategy']['USD_PER_TRADE']))
        raw_b = usd / price_b
        raw_y = usd / price_y

        info = await self.exchange_cache.get(bsym, ysym)
        b_info = info.get('binance', {}) or {}
        y_info = info.get('bybit', {}) or {}

        b_step = Decimal(str(b_info.get('stepSize') or 1e-8))
        y_step = Decimal(str(y_info.get('stepSize') or 1e-8))
        b_minQty = Decimal(str(b_info.get('minQty') or 0))
        y_minQty = Decimal(str(y_info.get('minQty') or 0))
        b_minNot = Decimal(str(b_info.get('minNotional') or 0))
        y_minNot = Decimal(str(y_info.get('minNotional') or 0))

        b_qty = Decimal(str(decimal_floor_to_step(raw_b, b_step)))
        y_qty = Decimal(str(decimal_floor_to_step(raw_y, y_step)))
        qty = min(b_qty, y_qty)

        if qty <= 0:
            logger.warning('Calculated qty <=0 for {} (b_qty={},y_qty={})', pair, b_qty, y_qty)
            return None
        if qty < b_minQty or qty < y_minQty:
            logger.warning('qty {} smaller than minQty (binance {}, bybit {})', qty, b_minQty, y_minQty)
            return None
        if qty * price_b < b_minNot or qty * price_y < y_minNot:
            logger.warning('Notional below minNotional for {} (qty={})', pair, qty)
            return None

        final_step = max(float(b_step), float(y_step))
        qty_f = decimal_floor_to_step(float(qty), final_step)
        qty = Decimal(str(qty_f))
        if qty <= 0:
            return None
        return float(qty)

    async def _place_binance_market_buy(self, symbol, qty):
        def f():
            return self.b_client.futures_create_order(symbol=symbol, side='BUY', type='MARKET', quantity=qty)
        return await asyncio.to_thread(f)

    async def _place_binance_market_sell(self, symbol, qty):
        def f():
            return self.b_client.futures_create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty)
        return await asyncio.to_thread(f)

    async def _place_bybit_market_sell(self, symbol, qty, reduce_only=False):
        def f():
            return self.y_client.place_active_order(symbol=symbol, side='Sell', order_type='Market', qty=qty, time_in_force='ImmediateOrCancel', reduce_only=reduce_only)
        return await asyncio.to_thread(f)

    async def _place_bybit_market_buy(self, symbol, qty, reduce_only=False):
        def f():
            return self.y_client.place_active_order(symbol=symbol, side='Buy', order_type='Market', qty=qty, time_in_force='ImmediateOrCancel', reduce_only=reduce_only)
        return await asyncio.to_thread(f)

    async def _confirm_binance_order(self, resp, symbol, max_retries=5):
        order_id = None
        if isinstance(resp, dict):
            order_id = resp.get('orderId') or resp.get('orderid') or resp.get('order_id')
        if not order_id:
            return resp
        for attempt in range(max_retries):
            def f():
                try:
                    return self.b_client.futures_get_order(symbol=symbol, orderId=order_id)
                except Exception as e:
                    return {'error': str(e)}
            r = await asyncio.to_thread(f)
            if isinstance(r, dict) and (r.get('status') == 'FILLED' or float(r.get('executedQty', 0)) > 0):
                return r
            await asyncio.sleep(0.5 * (2 ** attempt))
        return r

    async def _confirm_bybit_order(self, resp, symbol, max_retries=5):
        order_id = None
        if isinstance(resp, dict):
            res = resp.get('result') or resp
            order_id = res.get('order_id') or res.get('orderId') or res.get('order_id')
        if not order_id:
            return resp
        for attempt in range(max_retries):
            def f():
                try:
                    return self.y_client.query_active_order(symbol=symbol, order_id=order_id)
                except Exception as e:
                    return {'error': str(e)}
            r = await asyncio.to_thread(f)
            filled = 0
            try:
                if isinstance(r, dict):
                    if 'result' in r and isinstance(r['result'], dict):
                        filled = float(r['result'].get('qty', 0)) - float(r['result'].get('cum_exec_qty', 0))
                    else:
                        filled = float(r.get('filled_qty', 0) or 0)
            except Exception:
                filled = 0
            if filled > 0 or ('status' in r and r.get('status') == 'Filled'):
                return r
            await asyncio.sleep(0.5 * (2 ** attempt))
        return r

    async def _safe_rollback(self, pair, sides_filled):
        bsym, ysym = pair
        logger.warning('Safe rollback for {}, filled: {}', pair, sides_filled)
        tasks = []
        if sides_filled.get('binance', 0) > 0:
            q = sides_filled['binance']
            logger.info('Placing compensating SELL on Binance {} qty={}', bsym, q)
            tasks.append(asyncio.create_task(self._place_binance_market_sell(bsym, q)))
        if sides_filled.get('bybit', 0) > 0:
            q = sides_filled['bybit']
            logger.info('Placing compensating BUY on Bybit {} qty={}', ysym, q)
            tasks.append(asyncio.create_task(self._place_bybit_market_buy(ysym, q, reduce_only=True)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def open_pair(self, pair):
        async with self._pair_lock(pair):
            qty = await self.calc_qty(pair)
            if qty is None:
                logger.warning('Cannot open pair {}: qty calc failed', pair)
                return None
            bsym, ysym = pair
            logger.info('Attempting OPEN {} qty={}', pair, qty)
            if self.dry_run:
                logger.info('Dry-run mode: skipping real orders for {}', pair)
                return {'binance': 'dry', 'bybit': 'dry'}

            b_task = asyncio.create_task(self._place_binance_market_buy(bsym, qty))
            y_task = asyncio.create_task(self._place_bybit_market_sell(ysym, qty, reduce_only=False))

            done, pending = await asyncio.wait([b_task, y_task], return_when=asyncio.ALL_COMPLETED)

            b_resp = b_task.result() if b_task in done else None
            y_resp = y_task.result() if y_task in done else None

            b_conf = await self._confirm_binance_order(b_resp, bsym)
            y_conf = await self._confirm_bybit_order(y_resp, ysym)

            filled_b = 0.0
            filled_y = 0.0
            try:
                if isinstance(b_conf, dict):
                    filled_b = float(b_conf.get('executedQty', 0))
            except Exception:
                filled_b = 0.0
            try:
                if isinstance(y_conf, dict):
                    if 'result' in y_conf and isinstance(y_conf['result'], dict):
                        filled_y = float(y_conf['result'].get('qty', 0)) if y_conf['result'].get('qty') else float(y_conf['result'].get('filled_qty', 0) or 0)
                    else:
                        filled_y = float(y_conf.get('filled_qty', 0) or 0)
            except Exception:
                filled_y = 0.0

            if math.isclose(filled_b, qty, rel_tol=1e-9) and math.isclose(filled_y, qty, rel_tol=1e-9):
                logger.info('OPEN success for {} filled_b={} filled_y={}', pair, filled_b, filled_y)
                return {'binance': b_conf, 'bybit': y_conf}

            if (filled_b > 0 and filled_y == 0) or (filled_y > 0 and filled_b == 0):
                sides_filled = {'binance': filled_b, 'bybit': filled_y}
                logger.warning('Asymmetric opening detected (one side zero). Rolling back: {}', sides_filled)
                await self._safe_rollback(pair, sides_filled)
                logger.error('OPEN failed: one side filled while other not. Rolled back {}', pair)
                return None

            if filled_b > 0 and filled_y > 0:
                if abs(filled_b - filled_y) > qty * 0.01:
                    sides_filled = {'binance': filled_b, 'bybit': filled_y}
                    logger.warning('Significant asymmetry in fills {} — rolling back', sides_filled)
                    await self._safe_rollback(pair, sides_filled)
                    return None
                else:
                    logger.info('OPEN partial but acceptable for {}: filled_b={} filled_y={}', pair, filled_b, filled_y)
                    return {'binance': b_conf, 'bybit': y_conf}

            logger.error('OPEN failed: no fills on both sides for {}', pair)
            return None

    async def close_pair(self, pair):
        async with self._pair_lock(pair):
            qty = await self.calc_qty(pair)
            if qty is None:
                logger.warning('Cannot close pair {}: qty calc failed', pair)
                return None
            bsym, ysym = pair
            logger.info('Attempting CLOSE {} qty={}', pair, qty)
            if self.dry_run:
                logger.info('Dry-run mode: skipping real orders for {}', pair)
                return {'binance': 'dry', 'bybit': 'dry'}

            b_task = asyncio.create_task(self._place_binance_market_sell(bsym, qty))
            y_task = asyncio.create_task(self._place_bybit_market_buy(ysym, qty, reduce_only=True))
            done, pending = await asyncio.wait([b_task, y_task], return_when=asyncio.ALL_COMPLETED)

            b_resp = b_task.result() if b_task in done else None
            y_resp = y_task.result() if y_task in done else None

            b_conf = await self._confirm_binance_order(b_resp, bsym)
            y_conf = await self._confirm_bybit_order(y_resp, ysym)

            filled_b = 0.0
            filled_y = 0.0
            try:
                if isinstance(b_conf, dict):
                    filled_b = float(b_conf.get('executedQty', 0))
            except Exception:
                filled_b = 0.0
            try:
                if isinstance(y_conf, dict):
                    if 'result' in y_conf and isinstance(y_conf['result'], dict):
                        filled_y = float(y_conf['result'].get('qty', 0)) if y_conf['result'].get('qty') else float(y_conf['result'].get('filled_qty', 0) or 0)
                    else:
                        filled_y = float(y_conf.get('filled_qty', 0) or 0)
            except Exception:
                filled_y = 0.0

            if math.isclose(filled_b, qty, rel_tol=1e-9) and math.isclose(filled_y, qty, rel_tol=1e-9):
                logger.info('CLOSE success for {}', pair)
                return {'binance': b_conf, 'bybit': y_conf}

            if (filled_b > 0 and filled_y == 0) or (filled_y > 0 and filled_b == 0):
                sides_filled = {'binance': filled_b, 'bybit': filled_y}
                logger.warning('Asymmetric close detected (one side zero). Rolling back close: {}', sides_filled)
                await self._safe_rollback(pair, sides_filled)
                logger.error('CLOSE failed: one side closed while other not. Rolled back {}', pair)
                return None

            if filled_b > 0 and filled_y > 0:
                if abs(filled_b - filled_y) > qty * 0.01:
                    sides_filled = {'binance': filled_b, 'bybit': filled_y}
                    logger.warning('Significant asymmetry in close fills {} — rolling back', sides_filled)
                    await self._safe_rollback(pair, sides_filled)
                    return None
                else:
                    logger.info('CLOSE partial but acceptable for {}', pair)
                    return {'binance': b_conf, 'bybit': y_conf}

            logger.error('CLOSE failed: no fills on both sides for {}', pair)
            return None
