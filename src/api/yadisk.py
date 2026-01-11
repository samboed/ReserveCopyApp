import os

from urllib.parse import urljoin
from src.requests_misc import response_handler, RequestMethods

BASE_URL_YADISK = "https://cloud-api.yandex.net/v1/disk/resources/"
MAX_WORKERS_YADISK = 40


def download_file_to_ya_disk_from_url(headers: dict, path: str, url: str) -> tuple[bool, int]:
    response_handler_url = urljoin(BASE_URL_YADISK, "upload")
    response_handler_headers = headers
    response_handler_params = {"path": path, "url": url}
    res_response_handler, response_handler_status_code = response_handler(response_handler_url,
                                                                          response_handler_headers,
                                                                          response_handler_params,
                                                                          RequestMethods.POST,
                                                                          (202, ))
    if res_response_handler is False:
        return False, response_handler_status_code
    return True, response_handler_status_code


def get_file_info_from_ya_disk(headers: dict, path: str) -> tuple[bool, int] | tuple[dict, int]:
    response_handler_url = BASE_URL_YADISK
    response_handler_headers = headers
    response_handler_params = {"path": path}
    res_response_handler, response_handler_status_code = response_handler(response_handler_url,
                                                                          response_handler_headers,
                                                                          response_handler_params,
                                                                          RequestMethods.GET,
                                                                          (200, ))
    if res_response_handler is False:
        return False, response_handler_status_code
    return res_response_handler.json(), response_handler_status_code


def create_directory_in_yadisk(headers: dict, path: str) -> tuple[bool, int]:
    response_handler_url = BASE_URL_YADISK
    response_handler_headers = headers
    response_handler_params = {"path": path}
    res_response_handler, response_handler_status_code = response_handler(response_handler_url,
                                                                          response_handler_headers,
                                                                          response_handler_params,
                                                                          RequestMethods.PUT,
                                                                          (201, 409))
    if res_response_handler is False:
        return False, response_handler_status_code
    return True, response_handler_status_code


def uploading_file_to_ya_disk_from_url(headers: dict, path: str, url: str) -> tuple[bool, str, str, int] | tuple[tuple, str, str, int]:
    qty_upload_file_attempts = 10

    res_upload_file = False
    download_file_to_ya_disk_from_url_status_code = 0
    while res_upload_file is False and qty_upload_file_attempts != 0:
        res_upload_file, download_file_to_ya_disk_from_url_status_code = download_file_to_ya_disk_from_url(headers, path, url)
        qty_upload_file_attempts -= 1

    if qty_upload_file_attempts == 0:
        res_upload_file = False

    return res_upload_file, url, path, download_file_to_ya_disk_from_url_status_code


def getting_file_size_from_ya_disk(headers: dict, path: str) -> tuple[bool, str, int] | tuple[dict, str, int]:
    qty_get_size_upload_size_attempts = 10

    get_file_info_from_ya_disk_result = False
    get_file_info_from_ya_disk_status_code = 0
    while get_file_info_from_ya_disk_result is False and qty_get_size_upload_size_attempts != 0:
        get_file_info_from_ya_disk_result, get_file_info_from_ya_disk_status_code = get_file_info_from_ya_disk(headers, path)
        qty_get_size_upload_size_attempts -= 1

    if get_file_info_from_ya_disk_result is False:
        res_getting_file_size = False
    else:
        file_name = os.path.basename(path)
        res_getting_file_size = {file_name: get_file_info_from_ya_disk_result["size"]}

    return res_getting_file_size, path, get_file_info_from_ya_disk_status_code

