import os

from tqdm import trange
from concurrent import futures
from urllib.parse import urljoin

from src.defines import NAME_YADISK_RESULT_DIR, NAME_YADISK_RESERVE_CAT_DIR, PATH_TO_SIZE_CAT_PHOTOS_INFO_DIR
from src.misc import json_dump
from src.requests_misc import response_handler
from src.api.yadisk import (uploading_file_to_ya_disk_from_url, getting_file_size_from_ya_disk,
                            create_directory_in_yadisk, MAX_WORKERS_YADISK)

BASE_URL_CATAAS = "https://cataas.com"
SIZE_PHOTOS_INFO_JSON_FILE_NAME = "size_photos_info"


def get_qty_cats_from_cataas() -> tuple[bool, int] | tuple[int, int]:
    response_handler_url = urljoin(BASE_URL_CATAAS, "api/count")
    res_response_handler, response_handler_status_code = response_handler(response_handler_url)
    if res_response_handler is False:
        return False, response_handler_status_code
    return res_response_handler.json()["count"], response_handler_status_code


def get_data_cats_from_cataas(qty_cats: int) -> tuple[bool, int] | tuple[dict, int]:
    response_handler_url = urljoin(BASE_URL_CATAAS, "api/cats")
    response_handler_params = {"limit": qty_cats}
    res_response_handler, response_handler_status_code = response_handler(response_handler_url,
                                                                          params=response_handler_params)
    if res_response_handler is False:
        return False, response_handler_status_code
    return res_response_handler.json(), response_handler_status_code


def __forming_cat_upload_data(all_data_cats_list, image_text, path_ya_disk_reserve_cat_directory):
    cat_upload_data = []
    for cat_data in all_data_cats_list:
        cat_photo_name = f"{image_text}_cat_{cat_data["id"]}.jpg"
        path_ya_disk_photo = f"{path_ya_disk_reserve_cat_directory}/{cat_photo_name}"
        upload_url_photo = urljoin(BASE_URL_CATAAS, f"cat/{cat_data["id"]}/says/{image_text}")
        cat_upload_data.append((cat_photo_name, path_ya_disk_photo, upload_url_photo))
    return cat_upload_data


def reserve_copy_cats_from_cataas_to_yadisk(image_text: str, yadisk_access_token: str) -> bool:
    print("Launched reserve copy cats from cataas to yandex disk")
    yadisk_headers = {"Authorization": yadisk_access_token}

    print("Getting qty cats from cataas")
    qty_cats, get_qty_cats_from_cataas_status_code = get_qty_cats_from_cataas()
    if qty_cats is False:
        print(f"Fail get qty cats from cataas! <{get_qty_cats_from_cataas_status_code}>")
        return False

    print("Getting data cats from cataas")
    all_data_cats_list, get_all_data_cats_from_cataas_status_code = get_data_cats_from_cataas(qty_cats)
    if all_data_cats_list is False:
        print(f"Fail get all data cats from cataas! <{get_all_data_cats_from_cataas_status_code}>")
        return False

    path_yadisk_result_directory = NAME_YADISK_RESULT_DIR
    print(f"Creating {path_yadisk_result_directory} directory in yandex disk")
    create_directory_in_yadisk_result, create_directory_in_yadisk_status_code = create_directory_in_yadisk(yadisk_headers, path_yadisk_result_directory)
    if not create_directory_in_yadisk_result:
        print(f"Fail create directory {path_yadisk_result_directory} in yandex disk! <{create_directory_in_yadisk_status_code}>")
        return False

    path_yadisk_reserve_cat_directory = f"{NAME_YADISK_RESULT_DIR}/{NAME_YADISK_RESERVE_CAT_DIR}"
    print(f"Creating {path_yadisk_reserve_cat_directory} directory in yandex disk")
    create_directory_in_yadisk_result, create_directory_in_yadisk_status_code = create_directory_in_yadisk(yadisk_headers, path_yadisk_reserve_cat_directory)
    if not create_directory_in_yadisk_result:
        print(f"Fail create directory {path_yadisk_reserve_cat_directory} in yandex disk! <{create_directory_in_yadisk_status_code}>")
        return False

    cat_upload_data = __forming_cat_upload_data(all_data_cats_list, image_text, path_yadisk_reserve_cat_directory)

    # Uploading and getting size images
    size_photos_info = {}
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_YADISK) as executor_uploading_getting_size_photos:
        future_uploading_photos_list = []
        for cat_photo_name, path_ya_disk_photo, upload_url_photo in cat_upload_data:
            # Uploading size photos
            future_uploading_photos = executor_uploading_getting_size_photos.submit(uploading_file_to_ya_disk_from_url,
                                                                       yadisk_headers, path_ya_disk_photo,
                                                                       upload_url_photo)
            future_uploading_photos_list.append(future_uploading_photos)

        progress_uploading_photos = trange(qty_cats, desc="Uploading cats photos from cataas to yandex disk",
                                           colour="green")
        future_get_size_photos_list = []
        for future_uploading_photos in futures.as_completed(future_uploading_photos_list):
            upload_photos_result, upload_photos_url, upload_photos_path_yadisk, upload_photos_status_code = future_uploading_photos.result()
            if upload_photos_result is False:
                print(f"Fail uploading photos from {upload_photos_url} to {upload_photos_path_yadisk} yandex disk! <{upload_photos_status_code}>")
                executor_uploading_getting_size_photos.shutdown(wait=False)
                return False
            progress_uploading_photos.update()

            # Getting size photos
            future_get_size_photos = executor_uploading_getting_size_photos.submit(getting_file_size_from_ya_disk,
                                            yadisk_headers, upload_photos_path_yadisk)
            future_get_size_photos_list.append(future_get_size_photos)
        progress_uploading_photos.close()

        progress_get_size_photos = trange(qty_cats, desc="Getting size upload cat photos from yandex disk",
                                          colour="green")
        for future_get_size_photos in futures.as_completed(future_get_size_photos_list):
            get_size_photo_result, get_size_photo_path_yadisk, get_size_photo_status_code = future_get_size_photos.result()
            if get_size_photo_result is False:
                print(
                    f"Fail get size upload photo from {get_size_photo_path_yadisk} yandex disk! <{get_size_photo_status_code}>")
            else:
                size_photos_info.update(get_size_photo_result)
            progress_get_size_photos.update()
        progress_get_size_photos.close()

    json_file_name = f"{SIZE_PHOTOS_INFO_JSON_FILE_NAME}.json"
    path_data_info_cat_size_photos_info = os.path.join(PATH_TO_SIZE_CAT_PHOTOS_INFO_DIR, json_file_name)
    json_dump(path_data_info_cat_size_photos_info, size_photos_info)

    return True