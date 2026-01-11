import os
import json

def json_dump(path_to_json_file, data):
    if not os.path.exists(os.path.dirname(path_to_json_file)):
        os.makedirs(os.path.dirname(path_to_json_file))

    with open(path_to_json_file, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)
        print(f"Saved {path_to_json_file} json file")