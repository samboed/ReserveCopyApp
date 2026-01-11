import sys

from src.api.cataas import reserve_copy_cats_from_cataas_to_yadisk
from src.api.dogceo import reserve_copy_dogs_from_dogceo_to_yadisk

YADISK_ACCESS_TOKEN = ""

CAT_IMAGE_TEXT = "Hello"

RUN_RESERVE_COPY_CATS = False
RUN_RESERVE_COPY_DOGS = True


if __name__ == '__main__':
    if RUN_RESERVE_COPY_CATS:
        if not reserve_copy_cats_from_cataas_to_yadisk(CAT_IMAGE_TEXT, YADISK_ACCESS_TOKEN):
            sys.exit(1)
    if RUN_RESERVE_COPY_DOGS:
        if not reserve_copy_dogs_from_dogceo_to_yadisk(YADISK_ACCESS_TOKEN):
            sys.exit(1)
