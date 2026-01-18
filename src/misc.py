import os
import sys
import json
import logging


class CustomLoggingFormatter(logging.Formatter):
    LVL_FORMATS = {
        logging.DEBUG:    "\033[34m[%(asctime)s]\033[0m\033[32m[%(levelname)s]\033[0m %(message)s",
        logging.INFO:     "\033[34m[%(asctime)s]\033[0m\033[36m[%(levelname)s]\033[0m %(message)s",
        logging.WARNING:  "\033[34m[%(asctime)s]\033[0m\033[33m[%(levelname)s]\033[0m %(message)s",
        logging.ERROR:    "\033[34m[%(asctime)s]\033[0m\033[31m[%(levelname)s]\033[0m %(message)s",
        logging.CRITICAL: "\033[34m[%(asctime)s]\033[0m\033[35m[%(levelname)s]\033[0m %(message)s",
    }

    def format(self, record):
        return logging.Formatter(fmt=self.LVL_FORMATS.get(record.levelno)).format(record)


def init_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CustomLoggingFormatter())
    logger.addHandler(handler)


def json_dump(path_to_json_file, data):
    if not os.path.exists(os.path.dirname(path_to_json_file)):
        os.makedirs(os.path.dirname(path_to_json_file))

    with open(path_to_json_file, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)
        logging.info(f"Saved {path_to_json_file} json file")