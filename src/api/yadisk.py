import os

from urllib.parse import urljoin
from src.request_handler import request_handler, RequestMethods

BASE_URL_YADISK = "https://cloud-api.yandex.net/v1/disk/resources/"
MAX_WORKERS_YADISK = 40


class YaDiskAPI:
    def __init__(self, yadisk_access_token):
        self.__request_headers = {"Authorization": yadisk_access_token}

    def download_file_to_url(self, path: str, url: str) -> tuple[bool, int]:
        request_url = urljoin(BASE_URL_YADISK, "upload")
        request_headers = self.__request_headers
        request_params = {"path": path, "url": url}
        res_request, status_code_response = request_handler(request_url,
                                                             request_headers,
                                                             request_params,
                                                             RequestMethods.POST,
                                                             (202,))
        if res_request is False:
            return False, status_code_response
        return True, status_code_response

    def get_file_info(self, path: str) -> tuple[bool, int] | tuple[dict, int]:
        request_url = BASE_URL_YADISK
        request_headers = self.__request_headers
        request_params = {"path": path}
        res_request, status_code_response = request_handler(request_url,
                                                            request_headers,
                                                            request_params,
                                                            RequestMethods.GET,
                                                            (200,))
        if res_request is False:
            return False, status_code_response
        return res_request.json(), status_code_response

    def create_dir(self, path: str) -> tuple[bool, int]:
        request_url = BASE_URL_YADISK
        request_headers = self.__request_headers
        request_params = {"path": path}
        res_request, status_code_response = request_handler(request_url,
                                                            request_headers,
                                                            request_params,
                                                            RequestMethods.PUT,
                                                            (201, 409))
        if res_request is False:
            return False, status_code_response
        return True, status_code_response



def uploading_file_to_url(ya_api: YaDiskAPI, path: str, url: str, qty_upl_file_attempts: int = 10) \
        -> tuple[bool, str, str, int] | tuple[tuple, str, str, int]:
    res_upload_file = False
    download_file_to_yadisk_from_url_status_code = 0
    while res_upload_file is False and qty_upl_file_attempts != 0:
        res_upload_file, download_file_to_yadisk_from_url_status_code = ya_api.download_file_to_url(path, url)
        qty_upl_file_attempts -= 1

    if qty_upl_file_attempts == 0:
        res_upload_file = False

    return res_upload_file, path, url, download_file_to_yadisk_from_url_status_code


def getting_file_size(ya_api: YaDiskAPI, path: str, qty_get_size_file_attempts: int = 10) \
        -> tuple[bool, str, int] | tuple[dict, str, int]:
    get_file_info_from_yadisk_res = False
    get_file_info_from_yadisk_status_code = 0
    while get_file_info_from_yadisk_res is False and qty_get_size_file_attempts != 0:
        get_file_info_from_yadisk_res, get_file_info_from_yadisk_status_code = ya_api.get_file_info(path)
        qty_get_size_file_attempts -= 1

    if get_file_info_from_yadisk_res is False:
        res_getting_file_size = False
    else:
        file_name = os.path.basename(path)
        res_getting_file_size = {file_name: get_file_info_from_yadisk_res["size"]}

    return res_getting_file_size, path, get_file_info_from_yadisk_status_code

