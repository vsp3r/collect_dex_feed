import json
import os

def config_parse(config_file):
    # current_dir = os.path.dirname(__file__)
    # config_path = os.path.join(os.path.dirname(current_dir), config_file)
    config_path = os.path.join(os.path.curdir, config_file)
    with open(config_path) as f:
        data = json.load(f)
        return data