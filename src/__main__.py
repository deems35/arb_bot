import asyncio
import argparse
from loguru import logger
from src.config import load_config
from src.rest_scraper import RestScraper
from src.ws_manager import WSManager
from src.strategy import Strategy
from src.market_data import MarketData
from src.order_manager import OrderManager
from src.trade_state import TradeState
from src.logger_setup import setup_logger


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no real orders)')
    parser.add_argument('--config', default='config.yaml')
    args = parser.parse_args()

    cfg = load_config(args.config)
    # override runtime dry_run from CLI
    if args.dry_run:
        cfg.setdefault('runtime', {})['dry_run'] = True

    setup_logger(cfg.get('runtime', {}).get('log_level', 'INFO'))

    # create shared components
    market_data = MarketData()
    rest = RestScraper(cfg)
    ws = WSManager(cfg, market_data)

    # create order manager and state
    order_mgr = OrderManager(cfg, market_data, dry_run=bool(cfg.get('runtime', {}).get('dry_run', True)))
    state = TradeState()
    strategy = Strategy(cfg, market_data, order_mgr, state)

    # initial scan + start ws
    await rest.initial_scan()

    async def scan_callback(cache, top):
        await strategy.on_new_scan(cache, top)
        await ws.update_subscriptions(top)

    # run one immediate scan -> this will call scan_callback and subscribe WS to top pairs
    try:
        await rest._run_scan(callback=scan_callback)
    except Exception:
        logger.debug('Immediate _run_scan failed or unavailable; periodic scan will subscribe later')

    await ws.start()

    # debug printer
    async def print_market_data_loop():
        while True:
            try:
                tops = list(getattr(strategy, 'top_pairs', []))
                if not tops:
                    logger.debug('Debug MD: no top pairs yet')
                else:
                    for p in tops:
                        try:
                            md_bin = await market_data.get_l1((p[0], p[1], 'binance'))
                            md_ybt = await market_data.get_l1((p[0], p[1], 'bybit'))
                            # logger.info('MD {} binance: {} bybit: {}', p, md_bin, md_ybt)
                        except Exception as e:
                            logger.error('Debug MD fetch error for {}: {}', p, e)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception('Debug loop exception: {}', e)
            await asyncio.sleep(1)

    tasks = []
    try:
        tasks = [
            asyncio.create_task(rest.periodic_scan(scan_callback), name='rest_scan'),
            asyncio.create_task(strategy.run(), name='strategy'),
            asyncio.create_task(print_market_data_loop(), name='debug_printer'),
        ]
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info('Main gather cancelled')
    except KeyboardInterrupt:
        logger.info('KeyboardInterrupt received — shutting down')
    finally:
        for t in tasks:
            try:
                t.cancel()
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await ws.stop()


if __name__ == '__main__':
    asyncio.run(main())
