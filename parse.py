# This is a sample Python script.

import pickle
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from os import listdir
from os.path import isfile, join
from urllib.request import urlopen, Request

from bs4 import BeautifulSoup


# post_regexp = re.compile('<div class="wall_post_text">[^/]+</div>')


def main(min_length: int = 25, delay=1, cache_path='aneks.pkl'):
    aneks = set()
    offset = 0
    while True:
        response = urlopen(
            Request(
                url=f'https://vk.com/wall-149279263?own=1&offset={offset}',
                headers={
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
            )
        ).read().decode(encoding='windows-1251').replace('Expand text…', ' ')
        soup = BeautifulSoup(response, features="html.parser")
        # print(soup.prettify())
        all_aneks = tuple(
            map(
                lambda post: post.text,
                soup.find_all('div', {'class': 'wall_text'})
            )
        )
        all_remastered_aneks = tuple(
            map(
                lambda post: post.text,
                soup.find_all('div', {'class': 'wall_reply_text'})
            )
        )
        if len(all_aneks) == 0:
            break
        offset += len(all_aneks)
        for anek in filter(
                lambda anek: len(anek) >= min_length,
                all_remastered_aneks
        ):
            aneks.add(anek)
            print(anek)
        if (offset // 100 - (offset - len(all_aneks)) // 100) > 0:
            print('caching...')
            with open(cache_path, 'wb') as f:
                pickle.dump(aneks, f)
    # an = pickle.load(open('aneks.pkl', 'rb'))
    # print(an)


def merge(dir_path: str = 'caches', output: str = 'aneks.txt'):
    aneks = set()
    for file in filter(lambda file_path: isfile(file_path), map(lambda file_path: join(dir_path, file_path), listdir(dir_path))):
        with open(file, 'rb') as f:
            aneks = aneks.union(pickle.load(f))
    with open(output, 'w') as f:
        f.writelines(map(lambda anek: anek + '\n', aneks))


# Use a breakpoint in the code line below to debug your script.
# print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # main()
    merge()
    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
