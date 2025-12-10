import json
import os

def read_app_config(config_path=None):
    if config_path is None:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../app_config.json')
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r') as f:
        return json.load(f)

def get_log_level_from_config(config):
    level_str = config.get('log_level', 'INFO').upper()
    import logging
    return getattr(logging, level_str, logging.INFO)

