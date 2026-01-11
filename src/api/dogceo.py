import os

from tqdm import trange
from concurrent import futures
from urllib.parse import urljoin

from src.defines import NAME_YADISK_RESULT_DIR, NAME_YADISK_RESERVE_DOG_DIR, PATH_TO_SIZE_DOG_PHOTOS_INFO_DIR
from src.misc import json_dump
from src.requests_misc import response_handler
from src.api.yadisk import (uploading_file_to_ya_disk_from_url, getting_file_size_from_ya_disk,
                            create_directory_in_yadisk, MAX_WORKERS_YADISK)

BASE_URL_DOGCEO = "https://dog.ceo/api/"
SIZE_PHOTOS_INFO_JSON_FILE_NAME = "size_photos_info"
MAX_WORKERS_DOGCEO = 20


def get_dog_breeds_from_dogceo() -> tuple[bool, int] | tuple[dict, int]:
    response_handler_url = urljoin(BASE_URL_DOGCEO, "breeds/list/all")
    res_response_handler, response_handler_status_code = response_handler(response_handler_url)
    if res_response_handler is False:
        return False, response_handler_status_code
    return res_response_handler.json()["message"], response_handler_status_code


def get_dog_urls_from_dogceo(breed: str, subbreed: str = None)  -> tuple[bool, int] | tuple[tuple[tuple[str, str], dict], int]:
    if subbreed is None:
        url = f"breed/{breed}/images"
    else:
        url = f"breed/{breed}/{subbreed}/images"

    response_handler_url = urljoin(BASE_URL_DOGCEO, url)
    res_response_handler, response_handler_status_code = response_handler(response_handler_url)
    if res_response_handler is False:
        return False, response_handler_status_code
    return ((breed, subbreed), res_response_handler.json()["message"]), response_handler_status_code


def reserve_copy_dogs_from_dogceo_to_yadisk(yadisk_access_token: str) -> bool:
    print("Launched reserve copy dogs from dogceo to yandex disk")
    yadisk_headers = {"Authorization": yadisk_access_token}

    print("Getting dog breeds from dogceo")
    all_dog_breed_dict, get_dog_breeds_from_dogceo_status_code = get_dog_breeds_from_dogceo()
    if all_dog_breed_dict is False:
        print(f"Fail get dog breeds from dogceo! <{get_dog_breeds_from_dogceo_status_code}>")
        return False

    path_yadisk_result_directory = NAME_YADISK_RESULT_DIR
    print(f"Creating {path_yadisk_result_directory} directory in yandex disk")
    create_directory_in_yadisk_result, create_directory_in_yadisk_status_code = create_directory_in_yadisk(
        yadisk_headers, path_yadisk_result_directory)
    if not create_directory_in_yadisk_result:
        print(
            f"Fail create directory {path_yadisk_result_directory} in yandex disk! <{create_directory_in_yadisk_status_code}>")
        return False

    path_yadisk_reserve_dog_directory = f"{NAME_YADISK_RESULT_DIR}/{NAME_YADISK_RESERVE_DOG_DIR}"
    print(f"Creating {path_yadisk_reserve_dog_directory} directory in yandex disk")
    create_directory_in_yadisk_result, create_directory_in_yadisk_status_code = create_directory_in_yadisk(
        yadisk_headers, path_yadisk_reserve_dog_directory)
    if not create_directory_in_yadisk_result:
        print(
            f"Fail create directory {path_yadisk_reserve_dog_directory} in yandex disk! <{create_directory_in_yadisk_status_code}>")
        return False

    # Getting dog breed image urls
    qty_breed_images = 0
    breed_subbreed_urls_dict = {}
    future_get_breed_images_list = []
    executor_dogceo_get_breed_images = futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_DOGCEO)
    executor_yadisk_create_directories = futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_YADISK)

    for breed, subbreed_list in all_dog_breed_dict.items():
        path_yadisk_reserve_breed_directory = f"{path_yadisk_reserve_dog_directory}/{breed}"
        executor_yadisk_create_directories.submit(create_directory_in_yadisk, yadisk_headers, path_yadisk_reserve_breed_directory)
        if subbreed_list:
            for sub_breed in subbreed_list:
                path_yadisk_reserve_subbreed_directory = f"{path_yadisk_reserve_breed_directory}/{sub_breed}"
                executor_yadisk_create_directories.submit(create_directory_in_yadisk, yadisk_headers,
                                                           path_yadisk_reserve_subbreed_directory)

                future_get_breed_images = executor_dogceo_get_breed_images.submit(get_dog_urls_from_dogceo, breed, sub_breed)
                future_get_breed_images_list.append(future_get_breed_images)
        else:
            future_get_breed_images = executor_dogceo_get_breed_images.submit(get_dog_urls_from_dogceo, breed)
            future_get_breed_images_list.append(future_get_breed_images)

    qty_breed_and_subbreed = len(future_get_breed_images_list)
    progress_get_image_urls = trange(qty_breed_and_subbreed, desc="Getting dog breed image urls from dogceo",
                                     colour="green")
    for future_get_breed_images in futures.as_completed(future_get_breed_images_list):
        future_get_breed_images_result, get_breed_images_status_code = future_get_breed_images.result()
        if future_get_breed_images_result is False:
            print(f"Fail get dog breed image urls from dogceo! <{get_breed_images_status_code}>")
            executor_dogceo_get_breed_images.shutdown(wait=False)
            executor_yadisk_create_directories.shutdown(wait=False)
            return False
        breed_subbreed_key, image_url_list = future_get_breed_images_result
        breed_subbreed_urls_dict[breed_subbreed_key] = image_url_list
        qty_breed_images += len(image_url_list)
        progress_get_image_urls.update()

    executor_dogceo_get_breed_images.shutdown()
    executor_yadisk_create_directories.shutdown()
    progress_get_image_urls.close()

    # Uploading and getting size images
    size_photos_info = {}
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_YADISK) as executor_uploading_getting_size_photos:
        future_uploading_images_list = []
        for (breed, subbreed), upload_photo_url_list in breed_subbreed_urls_dict.items():
            path_ya_disk_reserve_directory = f"{path_yadisk_reserve_dog_directory}/{breed}"
            basename_photo = f"{breed}"
            if subbreed is not None:
                path_ya_disk_reserve_directory += f"/{subbreed}"
                basename_photo += f"-{subbreed}"
            for upload_photo_url in upload_photo_url_list:
                upload_photo_name = os.path.basename(upload_photo_url)
                photo_name = f"{basename_photo}_{upload_photo_name}"
                path_ya_disk_uploading_photo = f"{path_ya_disk_reserve_directory}/{photo_name}"

                # Uploading size photos
                future_uploading_images = executor_uploading_getting_size_photos.submit(uploading_file_to_ya_disk_from_url,
                                                                           yadisk_headers, path_ya_disk_uploading_photo,
                                                                           upload_photo_url)
                future_uploading_images_list.append(future_uploading_images)

        progress_uploading_images = trange(qty_breed_images, desc="Uploading dog photos from dogceo to yandex disk",
                                           colour="green")
        future_get_size_photos_list = []
        for future_uploading_images in futures.as_completed(future_uploading_images_list):
            upload_photos_result, upload_images_url, upload_images_path_yadisk, upload_images_status_code = future_uploading_images.result()
            if upload_photos_result is False:
                print(f"Fail uploading photo from {upload_images_url} to {upload_images_path_yadisk} yandex disk! <{upload_images_status_code}>")
                executor_uploading_getting_size_photos.shutdown(wait=False)
                return False
            progress_uploading_images.update()

            # Getting size photos
            future_get_size_photos = executor_uploading_getting_size_photos.submit(getting_file_size_from_ya_disk,
                                                                                   yadisk_headers,
                                                                                   upload_images_path_yadisk)
            future_get_size_photos_list.append(future_get_size_photos)
        progress_uploading_images.close()

        progress_get_size_photos = trange(qty_breed_images, desc="Getting size photos", colour="green")
        for future_get_size_photos in futures.as_completed(future_get_size_photos_list):
            get_size_photo_result, get_size_photo_path_yadisk, get_size_photo_status_code = future_get_size_photos.result()
            if get_size_photo_result is False:
                print(f"Fail get size upload photo from {get_size_photo_path_yadisk} yandex disk! <{get_size_photo_status_code}>")
            else:
                size_photos_info.update(get_size_photo_result)
            progress_get_size_photos.update()
        progress_get_size_photos.close()

    json_file_name = f"{SIZE_PHOTOS_INFO_JSON_FILE_NAME}.json"
    path_data_info_cat_size_photos_info = os.path.join(PATH_TO_SIZE_DOG_PHOTOS_INFO_DIR, json_file_name)
    json_dump(path_data_info_cat_size_photos_info, size_photos_info)

    return True