import asyncio
import json
import websockets
from loguru import logger
import os
from urllib.parse import urlparse
import socket
from contextlib import contextmanager

# --- SOCKS5 proxy bootstrap (uses ALL_PROXY or all_proxy env var) ---
# If ALL_PROXY is set like: socks5h://user:pass@host:port
# this will configure PySocks and replace socket.socket with socks.socksocket,
# so websockets and other libraries that use socket.socket will go through the proxy.
_proxy = os.getenv("ALL_PROXY") or os.getenv("all_proxy")
if _proxy:
    try:
        parsed = urlparse(_proxy)
        scheme = (parsed.scheme or "").lower()
        if scheme.startswith("socks"):
            try:
                import socks  # PySocks
                sockstype = socks.SOCKS5 if "socks5" in scheme else socks.SOCKS4
                proxy_host = parsed.hostname
                proxy_port = parsed.port or 1080
                proxy_user = parsed.username
                proxy_pass = parsed.password
                # socks5h => remote DNS (resolve on proxy)
                rdns = "socks5h" in scheme
                socks.set_default_proxy(sockstype, proxy_host, proxy_port, rdns, proxy_user, proxy_pass)
                socket.socket = socks.socksocket
                logger.info("SOCKS proxy enabled: {}://{}:{}", scheme, proxy_host, proxy_port)
            except ImportError:
                logger.warning("PySocks not installed — SOCKS proxy won't be used. Install PySocks in venv.")
            except Exception as e:
                logger.warning("Failed to enable SOCKS proxy: {}", e)
        else:
            logger.warning("ALL_PROXY set but scheme is not socks: {}", scheme)
    except Exception as e:
        logger.warning("Invalid ALL_PROXY value: {}", e)
# --- end SOCKS5 proxy bootstrap ---


# Context manager to temporarily disable HTTP(S) proxy env vars
@contextmanager
def _disable_http_proxy_env():
    keys = ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"]
    old = {}
    for k in keys:
        old[k] = os.environ.pop(k, None)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class WSManager:
    """
    WebSocket manager с дифф-подписками и безопасной обработкой входящих сообщений.
    """

    def __init__(self, cfg, market_data):
        self.cfg = cfg
        self.market_data = market_data
        self.active_pairs = set()
        self._tasks = []
        self._running = False

        # live ws objects (set while connection context is active)
        self._binance_ws_obj = None
        self._bybit_ws_obj = None
        self._lock = asyncio.Lock()

    async def start(self):
        self._running = True
        self._tasks = [
            asyncio.create_task(self._binance_ws(), name="binance_ws"),
            asyncio.create_task(self._bybit_ws(), name="bybit_ws"),
        ]

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def update_subscriptions(self, pairs):
        """
        Обновить подписки — отправляем только delta (subscribe/unsubscribe).
        """
        async with self._lock:
            new_set = set(pairs)
            to_add = new_set - self.active_pairs
            to_remove = self.active_pairs - new_set
            self.active_pairs = new_set

        logger.info('WS subscriptions updated: total={} to_add={} to_remove={}', len(self.active_pairs), len(to_add), len(to_remove))

        binance_add = [f"{b.lower()}@bookTicker" for b, y in to_add]
        binance_remove = [f"{b.lower()}@bookTicker" for b, y in to_remove]
        bybit_add = [f"orderbook.1.{y}" for b, y in to_add]
        bybit_remove = [f"orderbook.1.{y}" for b, y in to_remove]

        if self._binance_ws_obj is not None:
            try:
                if binance_add:
                    msg = {"method": "SUBSCRIBE", "params": binance_add, "id": 1}
                    await self._binance_ws_obj.send(json.dumps(msg))
                    logger.debug('Binance subscribed (delta send): {}', binance_add)
                if binance_remove:
                    msg = {"method": "UNSUBSCRIBE", "params": binance_remove, "id": 2}
                    await self._binance_ws_obj.send(json.dumps(msg))
                    logger.debug('Binance unsubscribed (delta send): {}', binance_remove)
            except Exception:
                logger.exception('Failed to send binance subscribe/unsubscribe')

        if self._bybit_ws_obj is not None:
            try:
                if bybit_add:
                    sub_msg = {"op": "subscribe", "args": bybit_add}
                    await self._bybit_ws_obj.send(json.dumps(sub_msg))
                    logger.debug('Bybit subscribed (delta send): {}', bybit_add)
                if bybit_remove:
                    unsub = {"op": "unsubscribe", "args": bybit_remove}
                    await self._bybit_ws_obj.send(json.dumps(unsub))
                    logger.debug('Bybit unsubscribed (delta send): {}', bybit_remove)
            except Exception:
                logger.exception('Failed to send bybit subscribe/unsubscribe')

    async def _binance_ws(self):
        url = 'wss://fstream.binance.com/ws'
        while self._running:
            try:
                # Important: disable HTTP_PROXY/HTTPS_PROXY env vars so websockets won't attempt HTTP proxy CONNECT.
                with _disable_http_proxy_env():
                    async with websockets.connect(url, max_size=2**23, ping_interval=20, ping_timeout=10) as ws:
                        self._binance_ws_obj = ws
                        logger.info('Binance ws connected')

                        try:
                            params = [f"{b.lower()}@bookTicker" for b, y in self.active_pairs]
                            if params:
                                msg = {"method": "SUBSCRIBE", "params": params, "id": 1}
                                await ws.send(json.dumps(msg))
                                logger.debug('Binance subscribed (on connect): {}', params)
                        except Exception:
                            logger.exception('Error while sending initial binance subscribe')

                        async for raw in ws:
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue

                            if 's' in msg and 'b' in msg and 'a' in msg:
                                symbol = msg['s']
                                pair = next(((b, y) for (b, y) in self.active_pairs if b.upper() == symbol.upper()), None)
                                if pair:
                                    bid = msg['b']
                                    ask = msg['a']
                                    await self.market_data.update((pair[0], pair[1], 'binance'), 'both', bid, ask)
                                else:
                                    logger.debug('Binance message for {} but no matching active pair', symbol)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception('Binance ws error')
                await asyncio.sleep(2)
            finally:
                self._binance_ws_obj = None

    async def _bybit_ws(self):
        url = 'wss://stream.bybit.com/v5/public/linear'
        while self._running:
            try:
                # disable HTTP proxy env vars for the connect call
                with _disable_http_proxy_env():
                    async with websockets.connect(url, max_size=2**23, ping_interval=20, ping_timeout=10) as ws:
                        self._bybit_ws_obj = ws
                        logger.info('Bybit v5 linear ws connected')

                        try:
                            if self.active_pairs:
                                args = [f"orderbook.1.{y}" for b, y in self.active_pairs]
                                if args:
                                    sub_msg = {"op": "subscribe", "args": args}
                                    await ws.send(json.dumps(sub_msg))
                                    logger.debug('Bybit subscribed (on connect): {}', args)
                        except Exception:
                            logger.exception('Error while sending initial bybit subscribe')

                        async for raw in ws:
                            try:
                                logger.debug('Bybit raw msg preview: {}', str(raw)[:400])
                            except Exception:
                                pass

                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue

                            topic = msg.get('topic') or msg.get('topicName') or msg.get('type')
                            data = msg.get('data') or msg.get('args') or msg.get('result') or {}
                            if not topic or not data:
                                continue

                            parts = str(topic).split('.')
                            if len(parts) >= 3:
                                sym = parts[-1]
                            else:
                                logger.debug('Bybit topic unexpected format: {}', topic)
                                continue

                            sym_norm = sym.upper()
                            pair = next(((b, y) for (b, y) in self.active_pairs if y.upper() == sym_norm), None)
                            if not pair:
                                logger.debug('Bybit message for {} but no matching active pair', sym)
                                continue

                            bid = ask = None
                            if isinstance(data, dict):
                                if 'b' in data and isinstance(data['b'], (list, tuple)) and len(data['b']) > 0:
                                    try:
                                        bid = data['b'][0][0]
                                    except Exception:
                                        bid = None
                                if 'a' in data and isinstance(data['a'], (list, tuple)) and len(data['a']) > 0:
                                    try:
                                        ask = data['a'][0][0]
                                    except Exception:
                                        ask = None

                                if bid is None and 'buy' in data and isinstance(data['buy'], (list, tuple)) and data['buy']:
                                    try:
                                        bid = data['buy'][0][0]
                                    except Exception:
                                        bid = None
                                if ask is None and 'sell' in data and isinstance(data['sell'], (list, tuple)) and data['sell']:
                                    try:
                                        ask = data['sell'][0][0]
                                    except Exception:
                                        ask = None

                                if bid is None and 'bids' in data and isinstance(data['bids'], (list, tuple)) and data['bids']:
                                    try:
                                        bid = data['bids'][0][0]
                                    except Exception:
                                        bid = None
                                if ask is None and 'asks' in data and isinstance(data['asks'], (list, tuple)) and data['asks']:
                                    try:
                                        ask = data['asks'][0][0]
                                    except Exception:
                                        ask = None

                                if (bid is None or ask is None) and 'tick' in data and isinstance(data['tick'], dict):
                                    tick = data['tick']
                                    if bid is None and 'b' in tick and tick['b']:
                                        try:
                                            bid = tick['b'][0][0]
                                        except Exception:
                                            pass
                                    if ask is None and 'a' in tick and tick['a']:
                                        try:
                                            ask = tick['a'][0][0]
                                        except Exception:
                                            pass

                            if bid is not None and ask is not None:
                                await self.market_data.update((pair[0], pair[1], 'bybit'), 'both', bid, ask)
                            else:
                                logger.debug('Bybit parsed data missing bid/ask for topic={}', topic)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception('Bybit ws error')
                await asyncio.sleep(2)
            finally:
                self._bybit_ws_obj = None
