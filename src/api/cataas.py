import logging
import os
import sys

import src.api.yadisk as yadisk_api

from tqdm import trange
from concurrent import futures
from urllib.parse import urljoin

from src.defines import NAME_YADISK_RES_COPY_DIR, NAME_YADISK_RES_CAT_DIR, PATH_TO_SIZE_CAT_PHOTOS_INFO_DIR
from src.misc import json_dump
from src.request_handler import request_handler


BASE_URL_CATAAS = "https://cataas.com"
SIZE_PHOTOS_INFO_JSON_FILE_NAME = "size_photos_info"


def get_qty_cats() -> tuple[bool, int] | tuple[int, int]:
    request_url = urljoin(BASE_URL_CATAAS, "api/count")
    res_request, status_code_response = request_handler(request_url)
    if res_request is False:
        return False, status_code_response
    return res_request.json()["count"], status_code_response


def get_data_cats(qty_cats: int) -> tuple[bool, int] | tuple[dict, int]:
    request_url = urljoin(BASE_URL_CATAAS, "api/cats")
    request_params = {"limit": qty_cats}
    res_request, status_code_response = request_handler(request_url, params=request_params)
    if res_request is False:
        return False, status_code_response
    return res_request.json(), status_code_response


def reserve_copy_cats_to_yadisk(yadisk_access_token: str, image_text: str = '') -> bool:
    if not yadisk_access_token:
        logging.warning(f"yadisk_access_token is required for reserve copy cats to yandex disk!")
        return False

    logging.info("Launched reserve copy cats from cataas to yandex disk")
    ya_disk = yadisk_api.YaDiskAPI(yadisk_access_token)

    logging.info("Getting qty cats from cataas")
    qty_cats, get_qty_cats_from_cataas_status_code = get_qty_cats()
    if qty_cats is False:
        logging.warning(f"Fail get qty cats from cataas! <{get_qty_cats_from_cataas_status_code}>")
        return False

    logging.info("Getting data cats from cataas")
    all_data_cats_list, get_all_data_cats_from_cataas_status_code = get_data_cats(qty_cats)
    if all_data_cats_list is False:
        logging.warning(f"Fail get all data cats from cataas! <{get_all_data_cats_from_cataas_status_code}>")
        return False

    path_yadisk_res_cat_dir = f"{NAME_YADISK_RES_COPY_DIR}/{NAME_YADISK_RES_CAT_DIR}"
    path_yadisk_create_dir = ''
    for create_yadisk_dir_name in path_yadisk_res_cat_dir.split('/'):
        path_yadisk_create_dir += f"{create_yadisk_dir_name}/"
        logging.info(f"Creating {path_yadisk_create_dir[:-1]} directory in yandex disk")
        create_yadisk_dir_res, create_yadisk_dir_status_code = ya_disk.create_dir(path_yadisk_create_dir)
        if not create_yadisk_dir_res:
            logging.warning(f"Fail create directory {path_yadisk_create_dir} in yandex disk! "
                          f"<{create_yadisk_dir_status_code}>")
            return False

    # Uploading and getting size images
    size_photos_info = {}
    with (futures.ThreadPoolExecutor(max_workers=yadisk_api.MAX_WORKERS_YADISK) as executor_yadisk_requests):
        future_upl_photos_list = []
        for cat_data in all_data_cats_list:
            cat_photo_name = f"{image_text}_cat_{cat_data["id"]}.jpg"
            path_ya_disk_photo = f"{path_yadisk_res_cat_dir}/{cat_photo_name}"
            upl_url_photo = urljoin(BASE_URL_CATAAS, f"cat/{cat_data["id"]}/says/{image_text}")

            # Uploading size photos
            future_upl_photos = executor_yadisk_requests.submit(yadisk_api.uploading_file_to_url, ya_disk,
                                                                path_ya_disk_photo, upl_url_photo)
            future_upl_photos_list.append(future_upl_photos)

        progress_upl_photos = trange(qty_cats, desc="Uploading cats photos from cataas to yandex disk",
                                     colour="green", file=sys.stdout)
        future_get_size_photos_list = []
        for future_upl_photos in futures.as_completed(future_upl_photos_list):
            upl_photos_res, upl_photos_path_yadisk, upl_photos_url, upl_photos_status_code =future_upl_photos.result()
            if upl_photos_res is False:
                logging.warning(f"Fail uploading photos from {upl_photos_url} to {upl_photos_path_yadisk} yandex disk!"
                              f" <{upl_photos_status_code}>")
                executor_yadisk_requests.shutdown(cancel_futures=True)
                return False
            progress_upl_photos.update()

            # Getting size photos
            future_get_size_photos = executor_yadisk_requests.submit(yadisk_api.getting_file_size,
                                                                     ya_disk, upl_photos_path_yadisk)
            future_get_size_photos_list.append(future_get_size_photos)
        progress_upl_photos.close()

        progress_get_size_photos = trange(qty_cats, desc="Getting size upload cat photos from yandex disk",
                                          colour="green", file=sys.stdout)
        for future_get_size_photos in futures.as_completed(future_get_size_photos_list):
            get_size_photo_res, get_size_photo_path_yadisk, get_size_photo_status_code = future_get_size_photos.result()
            if get_size_photo_res is False:
                logging.warning(
                    f"Fail get size upload photo from {get_size_photo_path_yadisk} yandex disk!"
                    f" <{get_size_photo_status_code}>")
            else:
                size_photos_info.update(get_size_photo_res)
            progress_get_size_photos.update()
        progress_get_size_photos.close()

    json_file_name = f"{SIZE_PHOTOS_INFO_JSON_FILE_NAME}.json"
    path_data_info_cat_size_photos_info = os.path.join(PATH_TO_SIZE_CAT_PHOTOS_INFO_DIR, json_file_name)
    json_dump(path_data_info_cat_size_photos_info, size_photos_info)

    return True