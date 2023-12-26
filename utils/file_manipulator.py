import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), ".."))

import json

def write_to_file(data: dict=None, name: str=None) -> None:
    """
    Function to write json data to file
    If file does not exist, create a new one and add data to it
    If file already have data, do nothing.
    """
    if not os.path.isfile(name):
        with open(name, 'w', encoding='utf-8') as file:
            file.write(json.dumps(data))
    else:
        print(f"File {name} already exists.")
