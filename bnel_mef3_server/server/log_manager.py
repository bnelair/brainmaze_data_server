import logging
import os
from datetime import datetime

def setup_logging(log_dir, log_level=logging.INFO):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    log_file = os.path.join(log_dir, f'server_{timestamp}.log')
    log_format = '%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s'
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter(log_format))
    root_logger = logging.getLogger()
    # Remove all handlers first (for repeated tests/reloads)
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)
    return log_file

def get_logger(name: str):
    return logging.getLogger(name)
