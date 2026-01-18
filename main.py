import sys
import src.api.cataas as cataas_api
import src.api.dogceo as dogceo_api

from src.misc import init_logging

YOUR_YADISK_ACCESS_TOKEN = ""

CAT_IMAGE_TEXT = "Hello"

RUN_RESERVE_COPY_CATS = True
RUN_RESERVE_COPY_DOGS = True


if __name__ == '__main__':
    init_logging()
    if RUN_RESERVE_COPY_CATS:
        if not cataas_api.reserve_copy_cats_to_yadisk(YOUR_YADISK_ACCESS_TOKEN, CAT_IMAGE_TEXT):
            sys.exit(1)
    if RUN_RESERVE_COPY_DOGS:
        if not dogceo_api.reserve_copy_dogs_to_yadisk(YOUR_YADISK_ACCESS_TOKEN):
            sys.exit(1)
