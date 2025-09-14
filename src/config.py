import os
import yaml
from dotenv import load_dotenv


def load_config(path: str = 'config.yaml'):
    load_dotenv()
    with open(path, 'r') as f:
        cfg = yaml.safe_load(f)

    cfg.setdefault('api', {})
    cfg['api']['binance_api_key'] = os.getenv('BINANCE_API_KEY') or cfg['api'].get('binance_api_key')
    cfg['api']['binance_api_secret'] = os.getenv('BINANCE_API_SECRET') or cfg['api'].get('binance_api_secret')
    cfg['api']['bybit_api_key'] = os.getenv('BYBIT_API_KEY') or cfg['api'].get('bybit_api_key')
    cfg['api']['bybit_api_secret'] = os.getenv('BYBIT_API_SECRET') or cfg['api'].get('bybit_api_secret')

    cfg.setdefault('runtime', {})
    return cfg
