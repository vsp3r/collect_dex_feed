from typing import List
import yaml
import os
import sys
import feeds
    
def check_directory_arg():
    for x in sys.argv:
        print('printing arg')
        print(x)
    
    if len(sys.argv) > 1:
        dir_name = sys.argv[1]

        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"Directory '{dir_name}' created.")
            return dir_name
        else:
            raise Exception(f"Directory '{dir_name}' already exists.")
    else:
        raise Exception("No directory name provided as argument.")



def config_parse(config_file):
    config_path = os.path.join(os.path.curdir, config_file)
    
    if not os.path.exists(config_path):
        raise Exception(f"'{config_path}' not found.")


    with open(config_path) as f:
        config = yaml.safe_load(f)
        return config
    

def config_validator(config):
    if type(config) is not dict:
        raise Exception("Configuration file contents should be a dict object")
    
    try:
        __validate_object(config, "Symbols")
        __validate_object(config, "Exchanges")
        __validate_exchanges(config, config['Exchanges'])
        return True 
    except Exception as e:
        print(f"Exception occured {e}. Halting")
        raise e



def __validate_exchanges(config, exchanges: List):
    missing_connectors = []

    for exchange in exchanges:
        exchange = exchange[0].upper() + exchange[1:].lower()
        connector_class_name = f"{exchange}Connector"
        if not hasattr(feeds, connector_class_name):
            missing_connectors.append(exchange)

    if missing_connectors:
        raise ImportError(f"No connector classes found for the following exchanges: {', '.join(missing_connectors)}")


def __validate_object(config, section):
    obj = config.get(section)
    if not obj or type(obj) is not list or not obj:
        raise Exception(f"The '{section}' field must contain at least one item and be a list.")

