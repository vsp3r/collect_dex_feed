import json
import os
import sys

def config_parse(config_file):
    # current_dir = os.path.dirname(__file__)
    # config_path = os.path.join(os.path.dirname(current_dir), config_file)
    config_path = os.path.join(os.path.curdir, config_file)
    with open(config_path) as f:
        data = json.load(f)
        return data
    
def check_directory_arg():
    if len(sys.argv) > 1:
        dir_name = sys.argv[1]

        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"Directory '{dir_name}' created.")
            return dir_name
        else:
            print(f"Directory '{dir_name}' already exists.")
            return dir_name
    else:
        print("No directory name provided as argument.")
        sys.exit(1)