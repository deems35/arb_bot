import asyncio
import aiohttp
import json
import time
from urllib.parse import urlencode

from loguru import logger
from aiolimiter import AsyncLimiter

from src.utils.math_utils import compute_quantiles, log_diff_series, normalize_ts


class RestScraper:
    """
    RestScraper: собирает klines с Binance и Bybit, выполняет сканирование пар и
    вычисляет квантильные каналы.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.window = int(cfg['strategy']['WINDOW_LEN'])
        self.low = float(cfg['strategy']['LOW'])
        self.high = float(cfg['strategy']['HIGH'])
        self.exceed = float(cfg['strategy']['EXCEED'])
        self.refresh = int(cfg['strategy']['REFRESH_INTERVAL'])
        self.semaphore_limit = int(cfg['strategy'].get('SEMAPHORE_LIMIT', 10))
        self.anti_ban = float(cfg['strategy'].get('ANTI_BAN_DELAY', 0.05))
        self.pairs = self._load_pairs()
        self.cache = {}

        # rate limiter: requests per second (RPS)
        rps = int(cfg['strategy'].get('RATE_LIMIT_RPS', 20))
        # token bucket with capacity=rps per 1 second
        self._limiter = AsyncLimiter(rps, 1)

    def _load_pairs(self):
        pairs = []
        try:
            with open('binance-bybit.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 2:
                        b, y = parts[0], parts[1]
                        pairs.append((b, y))
        except FileNotFoundError:
            logger.error("binance-bybit.txt not found")
        return pairs

    # ----------------- symbol lists validation helpers (robust & concurrent) -----------------
    async def _fetch_binance_symbols(self, session):
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        try:
            async with self._limiter:
                async with session.get(url, timeout=15) as r:
                    text = await r.text()
                    if r.status != 200:
                        logger.warning("Binance symbols HTTP {} body preview: {}", r.status, text[:300])
                        return set()
                    try:
                        j = json.loads(text)
                    except Exception:
                        j = None
                    if not isinstance(j, dict):
                        logger.warning("Binance symbols: unexpected JSON shape")
                        return set()
                    symbols = set()
                    for s in j.get('symbols', []) or []:
                        if isinstance(s, dict):
                            sym = s.get('symbol')
                            if sym:
                                symbols.add(sym)
                    return symbols
        except Exception as e:
            logger.warning("Binance symbols fetch exception: {}", e)
            return set()

    async def _fetch_bybit_symbols(self, session):
        """
        Bulk fetch Bybit v5 instruments (category=linear). Filter strictly:
        - quoteCoin == 'USDT'
        - contractType == 'LinearPerpetual'
        - status == 'Trading'

        Collect full symbol from entry['symbol'] (if present).
        """
        url = 'https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000'
        try:
            async with self._limiter:
                async with session.get(url, timeout=20) as r:
                    text = await r.text()
                    if r.status != 200:
                        logger.warning("Bybit bulk symbols HTTP {} preview: {}", r.status, text[:300])
                        return set()
                    try:
                        j = json.loads(text)
                    except Exception:
                        j = None
                    if not isinstance(j, dict):
                        logger.warning("Bybit symbols: unexpected JSON shape")
                        return set()
                    symbols = set()
                    res = j.get('result') or {}
                    arr = res.get('list') if isinstance(res, dict) else []
                    if isinstance(arr, (list, tuple)):
                        for entry in arr:
                            if not isinstance(entry, dict):
                                continue
                            # Strict filter according to user's requirement:
                            quote = entry.get('quoteCoin') or entry.get('quote')
                            ctype = entry.get('contractType') or entry.get('contract_type') or entry.get('contract')
                            status = entry.get('status')
                            # Accept only USDT perpetuals that are trading
                            if (quote and str(quote).upper() == 'USDT' and
                                ctype and ('PERPETUAL' in str(ctype).upper() or 'LINEARPERPETUAL' in str(ctype).upper()) and
                                status and str(status).lower() == 'trading'):
                                sym = entry.get('symbol') or entry.get('name')
                                if sym:
                                    symbols.add(sym)
                    return symbols
        except Exception as e:
            logger.warning("Bybit symbols fetch exception: {}", e)
            return set()

    async def _validate_pairs(self, session):
        """
        Robust validation: for Binance do bulk check; for Bybit do per-symbol checks
        (direct symbol query in linear) with fallback search by baseCoin.

        Uses exact full-symbol comparison (case-insensitive by using .upper()).
        """
        try:
            binance_syms = await self._fetch_binance_symbols(session)
        except Exception as e:
            logger.warning("Binance symbols fetch failed: {}", e)
            binance_syms = set()

        try:
            bybit_bulk_syms = await self._fetch_bybit_symbols(session)
        except Exception as e:
            logger.warning("Bybit bulk symbols fetch failed: {}", e)
            bybit_bulk_syms = set()

        # normalize sets to upper for robust exact matching
        binance_set_upper = {s.upper() for s in binance_syms}
        bybit_set_upper = {s.upper() for s in bybit_bulk_syms}

        valid = []
        invalid = []
        sem = asyncio.Semaphore(self.semaphore_limit)

        async def check_pair(b, y):
            # exact full-symbol check (use uppercase for equality)
            ok_b = None
            ok_y = None

            try:
                ok_b = True if (b and b.upper() in binance_set_upper) else False
            except Exception:
                ok_b = None

            # quick check from bulk bybit list first
            try:
                if y and y.upper() in bybit_set_upper:
                    ok_y = True
                    return (b, y, ok_b, ok_y)
            except Exception:
                pass

            # If not found in bulk or bulk not available, try direct bybit symbol query
            async def check_bybit_symbol(sym):
                # Query single symbol via instruments-info
                qs = urlencode({'category': 'linear', 'symbol': sym})
                url = f'https://api.bybit.com/v5/market/instruments-info?{qs}'
                try:
                    async with self._limiter:
                        async with session.get(url, timeout=12) as r:
                            if r.status != 200:
                                return None
                            txt = await r.text()
                            try:
                                j = json.loads(txt)
                            except Exception:
                                j = None
                            res = j.get('result') if isinstance(j, dict) else j
                            arr = res.get('list') if isinstance(res, dict) else (res if isinstance(res, list) else [])
                            if isinstance(arr, (list, tuple)) and len(arr) > 0:
                                for ent in arr:
                                    if not isinstance(ent, dict):
                                        continue
                                    s = ent.get('symbol') or ent.get('name')
                                    # also ensure this instrument matches the strict filters
                                    quote = ent.get('quoteCoin') or ent.get('quote')
                                    ctype = ent.get('contractType') or ent.get('contract_type') or ent.get('contract')
                                    status = ent.get('status')
                                    if s and s.upper() == str(sym).upper():
                                        if (quote and str(quote).upper() == 'USDT' and
                                            ctype and ('PERPETUAL' in str(ctype).upper() or 'LINEARPERPETUAL' in str(ctype).upper()) and
                                            status and str(status).lower() == 'trading'):
                                            return True
                                        else:
                                            return False
                                return False
                            return False
                except Exception:
                    return None

            async def check_bybit_by_base(sym):
                # fallback: query by baseCoin (from user's example)
                base = sym.upper()
                for suf in ('USDT', 'USD', 'PERP'):
                    if base.endswith(suf):
                        base = base[:-len(suf)]
                        break
                if not base:
                    return None
                qs = urlencode({'category': 'linear', 'baseCoin': base, 'limit': 500})
                url = f'https://api.bybit.com/v5/market/instruments-info?{qs}'
                try:
                    async with self._limiter:
                        async with session.get(url, timeout=15) as r:
                            if r.status != 200:
                                return None
                            txt = await r.text()
                            try:
                                j = json.loads(txt)
                            except Exception:
                                j = None
                            res = j.get('result') if isinstance(j, dict) else j
                            arr = res.get('list') if isinstance(res, dict) else (res if isinstance(res, list) else [])
                            if isinstance(arr, (list, tuple)) and len(arr) > 0:
                                for ent in arr:
                                    if not isinstance(ent, dict):
                                        continue
                                    s = ent.get('symbol') or ent.get('name')
                                    quote = ent.get('quoteCoin') or ent.get('quote')
                                    ctype = ent.get('contractType') or ent.get('contract_type') or ent.get('contract')
                                    status = ent.get('status')
                                    if s and s.upper() == str(sym).upper():
                                        if (quote and str(quote).upper() == 'USDT' and
                                            ctype and ('PERPETUAL' in str(ctype).upper() or 'LINEARPERPETUAL' in str(ctype).upper()) and
                                            status and str(status).lower() == 'trading'):
                                            return True
                                        else:
                                            return False
                                return False
                            return False
                except Exception:
                    return None

            # protected by semaphore to limit concurrency
            async with sem:
                try:
                    ok = await check_bybit_symbol(y)
                    if ok is True:
                        ok_y = True
                    elif ok is False:
                        ok2 = await check_bybit_by_base(y)
                        if ok2 is True:
                            ok_y = True
                        elif ok2 is False:
                            ok_y = False
                        else:
                            ok_y = None
                    else:
                        # ok is None -> network/other failure
                        ok_y = None
                except Exception as e:
                    logger.warning("Bybit validation error for {}: {}", y, e)
                    ok_y = None

            return (b, y, ok_b, ok_y)

        # spawn tasks
        tasks = [asyncio.create_task(check_pair(b, y)) for b, y in list(self.pairs)]
        # gather in chunks to avoid memory spike
        results = []
        chunk_size = max(1, self.semaphore_limit * 3)
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            try:
                chunk_res = await asyncio.gather(*chunk)
            except Exception as e:
                logger.warning("Validation chunk gather exception: {}", e)
                # collect partial results from chunk tasks that finished
                chunk_res = []
                for t in chunk:
                    if t.done() and not t.cancelled():
                        try:
                            chunk_res.append(t.result())
                        except Exception:
                            pass
            results.extend(chunk_res)

        for item in results:
            if not item:
                continue
            b, y, ok_b, ok_y = item
            if ok_b is True and ok_y is True:
                valid.append((b, y))
            else:
                invalid.append((b, y, ok_b, ok_y))

        removed = len(self.pairs) - len(valid)
        if removed > 0:
            logger.info("Validation removed {} pairs that are not present on both exchanges", removed)
            try:
                with open('bad_pairs.txt', 'w', encoding='utf-8') as f:
                    for it in invalid:
                        f.write(f"{it[0]},{it[1]},binance_ok={it[2]},bybit_ok={it[3]}\n")
            except Exception as e:
                logger.warning("Failed to write bad_pairs.txt: {}", e)
        self.pairs = valid

    # ----------------- kline fetchers with retry/backoff -----------------
    async def fetch_binance_k(self, session, symbol):
        url = f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={self.window}'
        max_attempts = 3
        backoff = 0.6
        for attempt in range(1, max_attempts + 1):
            try:
                async with self._limiter:
                    async with session.get(url, timeout=12) as r:
                        text = await r.text()
                        if r.status != 200:
                            logger.warning("Binance kline HTTP {} for {} (attempt {}). Body preview: {}", r.status, symbol, attempt, text[:200])
                            await asyncio.sleep(backoff * attempt)
                            continue
                        try:
                            j = json.loads(text)
                        except Exception:
                            j = None
                        if isinstance(j, dict) and ('code' in j or 'msg' in j):
                            logger.warning("Binance returned error for {}: {} (attempt {})", symbol, str(j)[:300], attempt)
                            await asyncio.sleep(backoff * attempt)
                            continue
                        if not isinstance(j, (list, tuple)):
                            logger.warning("Binance unexpected kline shape for {}: {} (attempt {})", symbol, type(j), attempt)
                            await asyncio.sleep(backoff * attempt)
                            continue
                        cleaned = []
                        for d in j:
                            try:
                                ts_raw = d[0]
                                close_price = d[4]
                                ts = normalize_ts(int(ts_raw))
                                cleaned.append((ts, float(close_price)))
                            except Exception:
                                continue
                        cleaned.sort(key=lambda x: x[0])
                        return cleaned
            except asyncio.TimeoutError:
                logger.warning("Binance kline timeout for {} (attempt {})", symbol, attempt)
                await asyncio.sleep(backoff * attempt)
            except Exception as e:
                logger.warning("Binance kline fetch exception for {}: {} (attempt {})", symbol, e, attempt)
                await asyncio.sleep(backoff * attempt)
        logger.debug("Binance kline give up for {} after {} attempts", symbol, max_attempts)
        return []

    async def fetch_bybit_k(self, session, symbol):
        url = f'https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=1&limit={self.window}'
        max_attempts = 3
        backoff = 0.6
        for attempt in range(1, max_attempts + 1):
            try:
                async with self._limiter:
                    async with session.get(url, timeout=12) as r:
                        text = await r.text()
                        try:
                            j = json.loads(text)
                        except Exception:
                            j = None
                        if r.status != 200:
                            logger.warning("Bybit kline HTTP {} for {} (attempt {}). Body preview: {}", r.status, symbol, attempt, text[:200])
                            await asyncio.sleep(backoff * attempt)
                            continue
                        if isinstance(j, dict) and ('retCode' in j or 'ret_code' in j):
                            code = j.get('retCode') or j.get('ret_code')
                            if code not in (0, None):
                                logger.warning("Bybit returned error for {}: code={} msg={} (attempt {})", symbol, code, j.get('retMsg') or j.get('ret_msg'), attempt)
                                await asyncio.sleep(backoff * attempt)
                                continue
                        arr = None
                        if isinstance(j, dict):
                            res = j.get('result') or j.get('data') or j
                            if isinstance(res, dict) and 'list' in res:
                                arr = res.get('list')
                            elif isinstance(res, (list, tuple)):
                                arr = res
                        if arr is None:
                            logger.warning("Bybit unexpected kline shape for {}: preview {} (attempt {})", symbol, str(j)[:200], attempt)
                            await asyncio.sleep(backoff * attempt)
                            continue
                        cleaned = []
                        if len(arr) == 0:
                            logger.debug("Bybit returned empty list for {} (attempt {})", symbol, attempt)
                            await asyncio.sleep(backoff * attempt)
                            continue
                        first = arr[0]
                        if isinstance(first, (list, tuple)):
                            for d in arr:
                                try:
                                    ts_raw = d[0]
                                    close = d[4]
                                    ts = normalize_ts(int(ts_raw))
                                    cleaned.append((ts, float(close)))
                                except Exception:
                                    continue
                        elif isinstance(first, dict):
                            for d in arr:
                                try:
                                    ts_raw = d.get('start') or d.get('startTime') or d.get('timestamp') or d.get('t')
                                    close = d.get('close') or d.get('closePrice') or d.get('c')
                                    if ts_raw is None or close is None:
                                        continue
                                    ts = normalize_ts(int(ts_raw))
                                    cleaned.append((ts, float(close)))
                                except Exception:
                                    continue
                        else:
                            logger.warning("Bybit kline element unexpected for {}: {} (attempt {})", symbol, type(first), attempt)
                            await asyncio.sleep(backoff * attempt)
                            continue
                        cleaned.sort(key=lambda x: x[0])
                        return cleaned
            except asyncio.TimeoutError:
                logger.warning("Bybit kline timeout for {} (attempt {})", symbol, attempt)
                await asyncio.sleep(backoff * attempt)
            except Exception as e:
                logger.warning("Bybit kline fetch exception for {}: {} (attempt {})", symbol, e, attempt)
                await asyncio.sleep(backoff * attempt)
        logger.debug("Bybit kline give up for {} after {} attempts", symbol, max_attempts)
        return []

    # ----------------- scan logic -----------------
    async def scan_pair(self, sem, session, pair):
        bsym, ysym = pair
        async with sem:
            try:
                b = await self.fetch_binance_k(session, bsym)
                await asyncio.sleep(self.anti_ban)
                y = await self.fetch_bybit_k(session, ysym)
            except Exception as e:
                logger.error("Failed fetch for {}: {}", pair, e)
                return None

        # quick retries for missing side(s)
        if not b or not y:
            retries = 2
            for i in range(retries):
                await asyncio.sleep(self.anti_ban * 2 + 0.2 * i)
                try:
                    if not b:
                        b = await self.fetch_binance_k(session, bsym)
                    if not y:
                        y = await self.fetch_bybit_k(session, ysym)
                except Exception:
                    pass
                if b and y:
                    break
            if not b or not y:
                logger.warning("Empty kline data for pair {} (binance={} bybit={}) — skipping", pair, len(b) if b else 0, len(y) if y else 0)
                return None

        b_map = {t: c for t, c in b}
        y_map = {t: c for t, c in y}
        common = sorted(set(b_map.keys()) & set(y_map.keys()))
        if len(common) < int(self.window * 0.5):
            logger.warning("Pair {} has small overlap: {}", pair, len(common))
            return None
        b_close = [b_map[t] for t in common]
        y_close = [y_map[t] for t in common]

        dif = log_diff_series(b_close, y_close)
        q_low, q_high = compute_quantiles(dif, self.low, self.high)
        width = q_high - q_low
        return (pair, q_low, q_high, width)

    async def initial_scan(self):
        # validate symbol list before scanning (best-effort)
        async with aiohttp.ClientSession() as session:
            try:
                await self._validate_pairs(session)
            except Exception:
                logger.debug("Pair validation skipped due to error")
        await self._run_scan(callback=None)

    async def _run_scan(self, callback=None):
        sem = asyncio.Semaphore(self.semaphore_limit)
        async with aiohttp.ClientSession() as session:
            tasks = [self.scan_pair(sem, session, p) for p in self.pairs]
            res = []
            chunk_size = max(1, self.semaphore_limit * 3)
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i:i + chunk_size]
                try:
                    chunk_res = await asyncio.gather(*chunk)
                    res.extend(chunk_res)
                except Exception as e:
                    logger.warning("Chunk gather exception: {}", e)
        tmp = {}
        for item in res:
            if not item:
                continue
            pair, q_low, q_high, width = item
            tmp[pair] = (q_low, q_high, width)
        self.cache = tmp
        candidates = [(p, vals[2]) for p, vals in tmp.items() if vals[2] > self.exceed]
        candidates.sort(key=lambda x: x[1], reverse=True)
        top = [p for p, _ in candidates[: self.cfg['strategy']['N_PAIRS_TRADE']]]
        logger.info("Scan done: found {} candidates, top: {}", len(candidates), top)
        if callback:
            try:
                await callback(tmp, top)
            except Exception as e:
                logger.warning("Scan callback error: {}", e)

    async def periodic_scan(self, callback):
        while True:
            now = time.time()
            sec = now - int(now)
            to_wait = (60 - int(now) % 60) + 1 - sec
            await asyncio.sleep(max(0.0, to_wait))
            try:
                await self._run_scan(callback)
            except Exception as e:
                logger.exception("Periodic scan exception: {}", e)
            await asyncio.sleep(max(0, self.refresh - 1))
