import os
import sys
import logging

import src.api.yadisk as yadisk_api

from tqdm import trange
from concurrent import futures
from urllib.parse import urljoin

from src.defines import NAME_YADISK_RES_COPY_DIR, NAME_YADISK_RES_DOG_DIR, PATH_TO_SIZE_DOG_PHOTOS_INFO_DIR
from src.misc import json_dump
from src.request_handler import request_handler

BASE_URL_DOGCEO = "https://dog.ceo/api/"
SIZE_PHOTOS_INFO_JSON_FILE_NAME = "size_photos_info"
MAX_WORKERS_DOGCEO = 20


def get_dog_breeds() -> tuple[bool, int] | tuple[dict, int]:
    request_url = urljoin(BASE_URL_DOGCEO, "breeds/list/all")
    res_request, status_code_response = request_handler(request_url)
    if res_request is False:
        return False, status_code_response
    return res_request.json()["message"], status_code_response


def get_dog_urls(breed: str, subbreed: str = None)  -> tuple[bool, int] | tuple[tuple[tuple[str, str], dict], int]:
    if subbreed is None:
        url = f"breed/{breed}/images"
    else:
        url = f"breed/{breed}/{subbreed}/images"

    request_url = urljoin(BASE_URL_DOGCEO, url)
    res_request, status_code_response = request_handler(request_url)
    if res_request is False:
        return False, status_code_response
    return ((breed, subbreed), res_request.json()["message"]), status_code_response


def reserve_copy_dogs_to_yadisk(yadisk_access_token: str) -> bool:
    if not yadisk_access_token:
        logging.warning(f"yadisk_access_token is required for reserve copy dogs to yandex disk!")
        return False

    logging.info("Launched reserve copy dogs from dogceo to yandex disk")
    ya_disk = yadisk_api.YaDiskAPI(yadisk_access_token)

    logging.info("Getting dog breeds from dogceo")
    all_dogceo_breed_dict, get_dogceo_breeds_status_code = get_dog_breeds()
    if all_dogceo_breed_dict is False:
        logging.warning(f"Fail get dog breeds from dogceo! <{get_dogceo_breeds_status_code}>")
        return False

    path_yadisk_res_dog_dir = f"{NAME_YADISK_RES_COPY_DIR}/{NAME_YADISK_RES_DOG_DIR}"
    path_yadisk_create_dir = ''
    for create_yadisk_dir_name in path_yadisk_res_dog_dir.split('/'):
        path_yadisk_create_dir += f"{create_yadisk_dir_name}/"
        logging.info(f"Creating {path_yadisk_create_dir[:-1]} directory in yandex disk")
        create_yadisk_dir_result, create_yadisk_dir_status_code = ya_disk.create_dir(path_yadisk_create_dir)
        if not create_yadisk_dir_result:
            logging.warning(f"Fail create directory {path_yadisk_create_dir} in yandex disk!"
                          f" <{create_yadisk_dir_status_code}>")
            return False

    # Getting dog breed image urls
    qty_breed_images = 0
    size_photos_info = {}
    with futures.ThreadPoolExecutor(max_workers=yadisk_api.MAX_WORKERS_YADISK) as executor_yadisk_requests:
        with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_DOGCEO) as executor_dogceo_requests:
            future_get_breed_images_list = []
            for breed, subbreed_list in all_dogceo_breed_dict.items():
                path_yadisk_res_breed_dir = f"{path_yadisk_res_dog_dir}/{breed}"
                executor_yadisk_requests.submit(ya_disk.create_dir, path_yadisk_res_breed_dir)
                if subbreed_list:
                    for sub_breed in subbreed_list:
                        path_yadisk_res_subbreed_dir = f"{path_yadisk_res_breed_dir}/{sub_breed}"
                        executor_yadisk_requests.submit(ya_disk.create_dir, path_yadisk_res_subbreed_dir)

                        future_get_breed_images = executor_dogceo_requests.submit(get_dog_urls, breed, sub_breed)
                        future_get_breed_images_list.append(future_get_breed_images)
                else:
                    future_get_breed_images = executor_dogceo_requests.submit(get_dog_urls, breed)
                    future_get_breed_images_list.append(future_get_breed_images)

            qty_breed_subbreed = len(future_get_breed_images_list)
            progress_get_image_urls = trange(qty_breed_subbreed, desc="Getting dog breed image urls from dogceo",
                                             colour="green", file=sys.stdout)
            future_upl_images_list = []
            for future_get_breed_images in futures.as_completed(future_get_breed_images_list):
                future_get_breed_images_result, get_breed_images_status_code = future_get_breed_images.result()
                if future_get_breed_images_result is False:
                    logging.warning(f"Fail get dog breed image urls from dogceo! <{get_breed_images_status_code}>")
                    executor_dogceo_requests.shutdown(cancel_futures=True)
                    executor_yadisk_requests.shutdown(cancel_futures=True)
                    return False
                (breed, subbreed), image_url_list = future_get_breed_images_result
                qty_breed_images += len(image_url_list)
                progress_get_image_urls.update()

                # Uploading size photos
                path_ya_disk_res_dir = f"{path_yadisk_res_dog_dir}/{breed}"
                basename_photo = f"{breed}"
                if subbreed is not None:
                    path_ya_disk_res_dir += f"/{subbreed}"
                    basename_photo += f"-{subbreed}"
                for upl_photo_url in image_url_list:
                    upl_photo_name = os.path.basename(upl_photo_url)
                    photo_name = f"{basename_photo}_{upl_photo_name}"
                    path_ya_disk_upl_photo = f"{path_ya_disk_res_dir}/{photo_name}"

                    future_upl_images = executor_yadisk_requests.submit(yadisk_api.uploading_file_to_url, ya_disk,
                                                                        path_ya_disk_upl_photo, upl_photo_url)
                    future_upl_images_list.append(future_upl_images)
            progress_get_image_urls.close()

        progress_upl_images = trange(qty_breed_images, desc="Uploading dog photos from dogceo to yandex disk",
                                     colour="green", file=sys.stdout)
        future_get_size_photos_list = []
        for future_upl_images in futures.as_completed(future_upl_images_list):
            upl_photos_res, upl_images_path_yadisk, upl_images_url, upl_images_status_code = future_upl_images.result()
            if upl_photos_res is False:
                logging.warning(f"Fail uploading photo from {upl_images_url} to {upl_images_path_yadisk} yandex disk!"
                              f" <{upl_images_status_code}>")
                executor_yadisk_requests.shutdown(cancel_futures=True)
                return False
            progress_upl_images.update()

            # Getting size photos
            future_get_size_photos = executor_yadisk_requests.submit(yadisk_api.getting_file_size,
                                                                     ya_disk, upl_images_path_yadisk)
            future_get_size_photos_list.append(future_get_size_photos)
        progress_upl_images.close()

        progress_get_size_photos = trange(qty_breed_images, desc="Getting size photos",
                                          colour="green", file=sys.stdout)
        for future_get_size_photos in futures.as_completed(future_get_size_photos_list):
            get_size_photo_res, get_size_photo_path_yadisk, get_size_photo_status_code = future_get_size_photos.result()
            if get_size_photo_res is False:
                logging.warning(f"Fail get size upload photo from {get_size_photo_path_yadisk} yandex disk!"
                              f" <{get_size_photo_status_code}>")
            else:
                size_photos_info.update(get_size_photo_res)
            progress_get_size_photos.update()
        progress_get_size_photos.close()

    json_file_name = f"{SIZE_PHOTOS_INFO_JSON_FILE_NAME}.json"
    path_data_info_cat_size_photos_info = os.path.join(PATH_TO_SIZE_DOG_PHOTOS_INFO_DIR, json_file_name)
    json_dump(path_data_info_cat_size_photos_info, size_photos_info)

    return True