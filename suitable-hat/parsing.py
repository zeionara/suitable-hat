import pickle
from os import listdir
from os.path import isfile, join
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

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


def _post_process_response(response: str):
    return response.replace('Expand text…', ' ')


def _update_cache(aneks: set, cache_path: str):
    with open(cache_path, 'wb') as f:
        pickle.dump(aneks, f)
    print('Cache was updated successfully')


def _get_posts(community_id: int, offset: int, class_name: str = 'wall_post_text'):
    response = _post_process_response(
        urlopen(
            Request(
                url=f'https://vk.com/wall-{community_id}?own=1&offset={offset}',
                headers=HEADERS
            )
        ).read().decode(encoding='windows-1251')
    )
    soup = BeautifulSoup(response, features="html.parser")
    for post in soup.find_all('div', {'class': class_name}):
        yield post.text


def parse(community_id: int = 85443458, min_length: int = 25, cache_delay: int = 100, cache_path='aneks.pkl'):
    aneks = set()
    offset = 0
    while True:
        all_aneks = tuple(_get_posts(community_id, offset))
        if len(all_aneks) == 0:
            break
        offset += len(all_aneks)
        for anek in filter(
                lambda anek: len(anek) >= min_length,
                all_aneks
        ):
            aneks.add(anek)
        if (offset // cache_delay - (offset - len(all_aneks)) // cache_delay) > 0:
            _update_cache(aneks, cache_path)
        print(f'Handled {offset} (+{len(all_aneks)}) aneks')
    _update_cache(aneks, cache_path)


def merge(dir_path: str = 'caches', file_path: str = 'aneks.txt'):
    aneks = set()
    for file in filter(
            lambda file_path_: isfile(file_path_),
            map(
                lambda file_path_: join(dir_path, file_path_),
                listdir(dir_path)
            )
    ):
        with open(file, 'rb') as f:
            aneks = aneks.union(pickle.load(f))
    with open(file_path, 'w') as f:
        f.writelines(map(lambda anek: anek + '\n', aneks))
