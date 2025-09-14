from loguru import logger
import sys


def setup_logger(level='INFO'):
    logger.remove()
    fmt = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    logger.add(sys.stdout, level=level, format=fmt)
    logger.add('bot.log', level=level, rotation='50 MB', retention='7 days', format=fmt)
