import os
import yaml
import pickle
import re
from itertools import chain
from os import listdir
from os.path import isfile, join
from time import sleep
from typing import List
from urllib.error import HTTPError
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
    # 'cookie': os.environ['COOKIE']
}

payload_regexp = re.compile('"<.+>"')
wall_post_id_regexp = re.compile('wall(?:_reply)?-[0-9]+_[0-9]+')
friend_id_regexp = re.compile('ava=1","([^"]+)"')
id_regexp = re.compile('id([0-9]{4,20})')

href_to_id_mappings = {}

delay = 1


def get_id(href: str):
    if href in href_to_id_mappings:
        return href_to_id_mappings[href]
    else:
        response = query(url=f'https://vk.com{href}')
        id = int(id_regexp.findall(response)[0])
        href_to_id_mappings[href] = id
        return id


def get_friends(id: int):
    try:
        response = payload_regexp.findall(
            query(url=f'https://vk.com/al_friends.php?act=load_friends_silent&al=1&gid=0&id={id}')
        )[0].replace('\\', '')
    except IndexError:
        return None
    sleep(delay)
    return friend_id_regexp.findall(response)


def get_communities(id: int):
    try:
        response = payload_regexp.findall(
            query(url=f'https://vk.com/al_fans.php?act=box&al=1&al_ad=0&oid={id}&tab=idols')
        )[0].replace('\\', '')
    except IndexError:
        return None
    bs = BeautifulSoup(response, features='html.parser')
    communities = tuple(
        map(
            lambda item: item['href'],
            bs.find_all('a', {'class': 'fans_idol_lnk'})
        )
    )
    sleep(delay)
    return communities


def _post_process_response(response: str):
    return response.replace('Expand text…', ' ')


def _update_cache(aneks: set, cache_path: str):
    with open(cache_path, 'wb') as f:
        pickle.dump(aneks, f)
    print('Cache was updated successfully')


def make_community_ref(comminity_id: int):
    return f'/wall-{comminity_id}'


def next_or_none(items, default=None):
    try:
        return next(items)
    except StopIteration:
        return default


def get_post_id(content):
    return int(content['id'].split('_')[-1])


def query(url: str):
    while True:
        try:
            return _post_process_response(
                urlopen(
                    Request(
                        url=url,
                        headers=HEADERS
                    )
                ).read().decode(encoding='windows-1251')
            )
        except HTTPError:
            print(f"Error querying url {url}. Trying again...")
            sleep(2)


def read_all(items: List, query: callable, parse_: callable):
    offset = len(items)
    while True:
        try:
            resp = query(offset)
        except IndexError:
            break
        new_entries = parse_(BeautifulSoup(resp, features='html.parser'))
        if len(new_entries) == 0:
            break
        else:
            offset += len(new_entries)
            items += new_entries
    return items


def get_likes(content):
    post_id = wall_post_id_regexp.findall(str(content))[0]
    likes = read_all(
        items=[],
        query=lambda offset: payload_regexp.findall(
            query(url=f'https://vk.com/wkview.php?act=show&offset={offset}&al=1&dmcah=&is_znav=1&ref=&w=likes/{post_id}')
        )[0].replace('\\', ''),
        parse_=lambda bs: tuple(
            map(
                lambda like: like['href'],
                bs.find_all('a', {'class': 'fans_fan_lnk'})
            )
        )
    )
    return likes


def get_remasterings(content, community_id: int):
    def parse_(data):
        return tuple(
            map(
                lambda reply: {
                    'author': next_or_none(
                        map(
                            lambda author: author.find_all('a', {'class': 'author'})[0]['href'],
                            reply.find_all('div', {'class': 'reply_author'})
                        )
                    ),
                    'text': next_or_none(
                        map(
                            lambda post: post.text,
                            reply.find_all('div', {'class': 'wall_reply_text'})
                        )
                    ),
                    'community': community_ref,
                    'likes': get_likes(reply)
                },
                data.find_all('div', {'class': 'reply_content'})
            )
        )

    community_ref = make_community_ref(community_id)
    post_id = get_post_id(content)
    remasterings = tuple(
        filter(
            lambda reply: reply['author'] is not None and reply['text'] is not None,
            read_all(
                items=list(parse_(content)),
                query=lambda offset: payload_regexp.findall(
                    query(url=f'https://vk.com/wall-{community_id}?act=get_post_replies&al=1&count=20&item_id={post_id}&offset={offset}&order=smart&prev_id={post_id}')
                )[0].replace('\\', ''),
                parse_=parse_
            )
        )
    )
    return remasterings


def _get_posts(community_id: int, offset: int):
    response = query(url=f'https://vk.com/wall-{community_id}?own=1&offset={offset}')
    soup = BeautifulSoup(response, features="html.parser")
    community_ref = make_community_ref(community_id)
    # start = time()
    aneks = tuple(
        map(
            lambda content: {
                'author': next_or_none(
                    map(
                        lambda author: author.find_all('a', {'class': 'wall_signed_by'})[0]['href'],
                        content.find_all('div', {'class': 'wall_signed'})
                    ),
                    default=next_or_none(
                        map(
                            lambda author: author.find_all('a', {'class': 'author'})[0]['href'],
                            content.find_all('h5', {'class': 'post_author'})
                        )
                    )
                ),
                'text': next_or_none(
                    map(
                        lambda post: post.text,
                        content.find_all('div', {'class': 'wall_post_text'})
                    )
                ),
                'community': community_ref,
                'likes': get_likes(content),
                'remasterings': get_remasterings(content, community_id=community_id)
            },
            soup.find_all('div', {'class': 'post'})
        )
    )
    # with open('aneks.pkl', 'rb') as f:
    #     aneks = pickle.load(f)
    # print(f'Handled aneks in {time() - start} seconds.')
    # hrefs = tuple(
    #     set(
    #         tuple(map(lambda item: item['author'], aneks)) +
    #         tuple(chain(*map(lambda item: map(lambda remastering: remastering['author'], item['remasterings']), aneks))) +
    #         tuple(chain(*map(lambda item: item['likes'], aneks))) +
    #         tuple(chain(*map(lambda item: chain(*map(lambda remastering: remastering['likes'], item['remasterings'])), aneks)))
    #     )
    # )[:10]
    # print(f'{len(hrefs)} users to handle')
    # start = time()
    # users = {
    #     href: {
    #         'communities': get_communities(get_id(href)),
    #         'friends': get_friends(get_id(href))
    #     }
    #     for href in
    #     hrefs
    # }
    # print(f'Handled users in {time() - start} seconds.')
    return {
        'aneks': aneks,
        'users': set(
            tuple(map(lambda item: item['author'], aneks)) +
            tuple(chain(*map(lambda item: map(lambda remastering: remastering['author'], item['remasterings']), aneks))) +
            tuple(chain(*map(lambda item: item['likes'], aneks))) +
            tuple(chain(*map(lambda item: chain(*map(lambda remastering: remastering['likes'], item['remasterings'])), aneks)))
        )
    }


def parse(community_id: int = 85443458, min_length: int = 25, offset: int = 0, cache_delay: int = 100, cache_path='aneks.pkl', remasterings: bool = False):
    aneks = {'aneks': [], 'users': set()}
    anek_texts = set()
    # offset = 0
    # id = get_id('/markysha_markysha')
    # print('id')
    # friends = get_friends(id)
    # communities = get_communities(id)
    while True:
        print(f'Offset = {offset}')
        try:
            aneks_ = _get_posts(community_id, offset)
        except HTTPError as e:
            _update_cache(aneks, cache_path)
            raise e
        if len(aneks_['aneks']) == 0:
            break
        offset += len(aneks_['aneks'])
        for anek in filter(
                lambda anek_: anek_['text'] is not None, aneks_['aneks']  # and len(anek_['text']) >= min_length, aneks_['aneks']
        ):
            if anek['text'] not in anek_texts:
                anek_texts.add(anek['text'])
                aneks['aneks'].append(anek)
        aneks['users'] = aneks_['users'].union(aneks['users'])
        if (offset // cache_delay - (offset - len(aneks_['aneks'])) // cache_delay) > 0:
            _update_cache(aneks, cache_path)
        print(aneks_)
        print(f'Handled {offset} (+{len(aneks_["aneks"])}) aneks')
    _update_cache(aneks, cache_path)


def merge(dir_path: str = 'caches', file_path: str = 'aneks.yml'):
    aneks = {'aneks': [], 'users': set()}
    anek_texts = set()
    for file in filter(
            lambda file_path_: isfile(file_path_),
            map(
                lambda file_path_: join(dir_path, file_path_),
                listdir(dir_path)
            )
    ):
        with open(file, 'rb') as f:
            # aneks = aneks.union(pickle.load(f))
            aneks_ = pickle.load(f)
        for anek in aneks_['aneks']:
            if anek['text'] not in anek_texts:
                anek_texts.add(anek['text'])
                anek['remasterings'] = list(anek['remasterings'])
                aneks['aneks'].append(anek)
        aneks['users'] = aneks_['users'].union(aneks['users'])

    aneks['users'] = list(aneks['users'])

    _update_cache(aneks, f"{file_path.split('.', 1)[0]}.pkl")

    with open(file_path, 'w') as f:
        yaml.dump(aneks, f, allow_unicode=True)
