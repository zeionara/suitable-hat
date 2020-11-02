import json
import re
from time import sleep
from typing import List
from urllib.error import HTTPError
from urllib.request import urlopen, Request

from bs4 import BeautifulSoup

from .string import post_process_response

HEADERS = {
    'authority': 'vk.com',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7'
}

http_error_delay = 2  # seconds
payload_regexp = re.compile('"<.+>"')


def query(url: str, as_json: bool = False, as_vk_payload: bool = False):
    assert not (as_json and as_vk_payload)
    result = None
    while True:
        try:
            result = post_process_response(
                urlopen(
                    Request(
                        url=url,
                        headers=HEADERS
                    )
                ).read().decode(encoding='windows-1251', errors='ignore')
            )
            break
        except HTTPError as e:
            if e.code != 404:
                print('Error sending query. Retrying...')
                sleep(http_error_delay)
            else:
                break
    return {} if result is None else (
        json.loads(result) if as_json else
        payload_regexp.findall(result)[0].replace('\\', '') if as_vk_payload else
        result
    )


def query_sequence(items: List, query_: callable, parse_: callable):
    offset = len(items)
    while True:
        try:
            resp = query_(offset)
        except IndexError:
            break
        new_entries = parse_(BeautifulSoup(resp, features='html.parser'))
        if len(new_entries) == 0:
            break
        else:
            offset += len(new_entries)
            items += new_entries
    return items
