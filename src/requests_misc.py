import requests

from enum import Enum


class RequestMethods(Enum):
    GET = 0
    PUT = 1
    POST = 2


def response_handler(url, headers=None, params=None, method: RequestMethods = RequestMethods.GET,
                     expect_response_status_code: tuple = (200,)):
    if headers is None:
        headers = {}
    if params is None:
        params = {}

    remaining_request_attempts = 5
    while remaining_request_attempts:
        try:
            match method:
                case RequestMethods.GET:
                    response_get_file_info = requests.get(url, headers=headers, params=params)
                case RequestMethods.PUT:
                    response_get_file_info = requests.put(url, headers=headers, params=params)
                case RequestMethods.POST:
                    response_get_file_info = requests.post(url, headers=headers, params=params)
            break
        except requests.exceptions.RequestException as ex:
            print(f"{ex} url={url}")
            remaining_request_attempts -= 1
    else:
        return False, 0

    if response_get_file_info.status_code not in expect_response_status_code:
        return False, response_get_file_info.status_code

    return response_get_file_info, response_get_file_info.status_code
