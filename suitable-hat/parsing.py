import os
import json
import yaml
import pickle
import re
from itertools import chain
from os import listdir
from os.path import isfile, join
from time import sleep, time
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
}

LOGIN_HEADERS = {
    'authority': 'login.vk.com',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'origin': 'https://vk.com',
    'content-type': 'application/x-www-form-urlencoded',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'iframe',
    'referer': 'https://vk.com/',
    'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7'
}

cookies = []

remixsid_regexp = re.compile('remixsid=[^;]+;')

current_cookie_id = 0

payload_regexp = re.compile('"<.+>"')
wall_post_id_regexp = re.compile('wall(?:_reply)?-[0-9]+_[0-9]+')
friend_id_regexp = re.compile('ava=1","([^"<]+)"')
id_regexp = re.compile('"user_id":([0-9]+)')

href_to_id_mappings = {}

delay = 1.0


def login():
    resp = urlopen(
        Request(
            url='https://login.vk.com/?act=login',
            headers=LOGIN_HEADERS,
            data=''.encode()
        )
    )
    cookies[0] = f"{LOGIN_HEADERS['cookie']}; {remixsid_regexp.findall(resp.getheader('Set-Cookie'))[0]}"


def get_ids(hrefs: iter):
   resp=query(url=f"https://api.vk.com/method/users.get?user_ids={','.join(href[1:] for href in hrefs)}&v=5.124&fields=id,screen_name&access_token={os.environ['VK_TOKEN']}")
   results = {}

   for item in json.loads(resp)['response']:
        if item['first_name'] != 'DELETED' and 'deactivated' not in item:
            # print(item)
            results[item['screen_name']] = {'id': item['id'], 'is-closed': item['is_closed'], 'is-deleted': False}
   for href in hrefs:
        if href[1:] not in results:
            results[href[1:]] = {'is-deleted': True, 'is-closed': False}
   return results


def get_friends_list(id: int):
   resp=query(url=f"https://api.vk.com/method/friends.get?user_id={id}&v=5.124&fields=screen_name&access_token={os.environ['VK_TOKEN']}")
   return [item['screen_name'] for item in json.loads(resp)['response']['items'] if item['first_name'] != 'DELETED' and 'deactivated' not in item]


def get_headers():
    # global HEADERS, current_cookie_id, cookies
    # HEADERS['cookie'] = cookies[current_cookie_id]
    # current_cookie_id = (current_cookie_id + 1) % len(cookies)
    return HEADERS

def get_id(href: str):
    if href in href_to_id_mappings:
        return href_to_id_mappings[href]
    else:
        response = query(url=f'https://vk.com{href}')
        # print(response)
        # print(f'https://vk.com{href}')
        matches = id_regexp.findall(response)
        print(f'Got ids for {href}: ', matches)
        
        try:
            id = int(matches[0])
        except IndexError:
            if href.startswith('/id'):
                try:
                    id = int(href.replace('/id', '', 1))
                except ValueError:
                    id = None
            else:
                id = None
        href_to_id_mappings[href] = id
        return id


def get_friends(id: int):
    try:
        response = payload_regexp.findall(
            query(url=f'https://vk.com/al_friends.php?act=load_friends_silent&al=1&gid=0&id={id}')
        )[0].replace('\\', '')
        return friend_id_regexp.findall(response)
    except IndexError:
        return None
    # finally:
    #     sleep(delay)


def get_communities(id: int):
    try:
        response = payload_regexp.findall(
            query(url=f'https://vk.com/al_fans.php?act=box&al=1&al_ad=0&oid={id}&tab=idols')
        )[0].replace('\\', '')
        bs = BeautifulSoup(response, features='html.parser')
        communities = list(
            map(
                lambda item: item['href'][1:],
                bs.find_all('a', {'class': 'fans_idol_lnk'})
            )
        )
        return communities
    except IndexError:
        return None
    # finally:
    #     sleep(delay)


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
                        headers=get_headers()
                    )
                ).read().decode(encoding='windows-1251', errors='ignore')
            )
        except HTTPError as e:
            if e.code != 404:
                print(e.__dict__)
                print(f"Error querying url {url}. Trying again...")
                sleep(2 * delay)
            else:
                return ''



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

    print(f'Found {len(tuple(aneks["users"]))} users')

    # with open(file_path, 'w') as f:
    #     yaml.dump(aneks, f, allow_unicode=True)

def load_users(cache_path: str = 'assets/baneks.pkl', file_path: str = 'assets/users.pkl', cache_delay: int = 100):
    # login()
    # print(delay)
    # for i in range(10):
    #     print(get_headers())
    with open(cache_path, 'rb') as f:
        aneks = pickle.load(f)
    users_ = aneks['users']
    del aneks
    # users_ = ['/mcluvin', '/gospes', '/agrishin', '/sergortm', '/lexajeas', '/id10562339', '/ksp256', '/poluektoff_08', '/id250088249', '/id57486417', '/katherinaklim', '/kateeeeeriina', '/mrbloodness', '/dimchu', '/idinaxyi31', '/id_hell_paradise', '/sugarhl', '/pinkamena', '/id83420082', '/id556218588', '/idrecon666', '/valera_popov_0704', '/oimygoddaddy', '/id210257337', '/77mike', '/id16449012', '/igor_z93', '/id161114435', '/msirkin', '/jon_pelegrim', '/id117934156', '/dlnkayt', '/fineguy', '/megamrazhoma', '/faerrx', '/valera.gorbunov', '/id32572564', '/battsn5', '/dimka26', '/noirshinobi', '/id500133915', '/pushkinalove', '/id192746934', '/morgol07', '/dlukich', '/ipezio', '/alkoforce', '/id106100939', '/id317006682', '/phephe975', '/akoretsk', '/id43721345', '/id270101922', '/id143522890', '/antusheva03', '/vovan7590', '/phantom87', '/id600994201', '/docm0t', '/doom04', '/id20929669', '/valentln1337', '/e_k_xrystik', '/ares3', '/id175277384', '/gggqggge', '/robbievil', '/andrey_b1999', '/fox_martin', '/kartavii_hren', '/daria_sakyra', '/id303227458', '/merkulow', '/yudinko', '/ad_with_ak', '/alya_dyra', '/id288088296', '/id14840489', '/alex_goldenmyer', '/chaechka_e', '/id3644923', '/drevniydraianec', '/ks.evtushenko', '/n.bewiga', '/naggets16', '/tea_sweet_t', '/drakosha_d', '/id563245194', '/skyinmymind', '/id188720379', '/id142901702', '/id138075134', '/whoa_m1', '/gordina1707', '/qucro', '/justrooit', '/id183295771', '/marsh_stanley_marsh', '/vanessa_super', '/anomalechka']
    # users_ = ['/lipatova96']
    # print(','.join(user[1:] for user in users_))
    # ids = {}
    # print(users_)
    chunk_size = 200
    i = 0
    start = time()
    n_chunks = len(users_) // chunk_size + 1
    for chunk in [users_[i*chunk_size:(i+1)*chunk_size] for i in range(n_chunks)]:
        if i % 2 == 1:
            ids_ = get_ids(chunk)
            ids = {}
            j = 0
            for key, value in ids_.items():
                # if key not in ids:
                if 'id' in value and not (value['is-closed'] or value['is-deleted']):
                    value['communities'] = get_communities(value['id'])
                    value['friends'] = get_friends_list(value['id'])
                print(f"{j}: {value}")
                ids[key] = value
                j += 1
            _update_cache(ids, f'assets/ids/{i*chunk_size}-{(i+1)*chunk_size}.pkl')
        i += 1
        print(f'Handled {i} / {n_chunks} in {time() - start} seconds')
        start = time()
    # print(chunks)
    # ids = get_ids(users_)
    # ids_for_next_stage = [item['id'] for item in ids.values() if not (item['is-closed'] or item['is-deleted'])]
    # print(len(ids_for_next_stage))
    _update_cache(ids, f'assets/ids/{i*chunk_size}-{(i+1)*chunk_size}.pkl')
    # n_total = len(users_)
    # n_closed_profiles = 0
    # i = 0
    # users = {}
    # start = time()
    # try:
    #     for user in users_:
    #         if user not in users:
    #             print(f'Adding {user}...')
    #             id_ = get_id(user)
    #             if id_ is None:
    #                 info = {'is-closed': True}
    #             else:
    #                 friends = get_friends(id_)
    #                 communities = get_communities(id_)
    #                 info = {
    #                     'id': id_,
    #                     'is-closed': friends is None and communities is None
    #                 }
    #                 if friends is not None:
    #                     info['friends'] = friends
    #                 if communities is not None:
    #                     info['communities'] = communities
    #             if info['is-closed']:
    #                 n_closed_profiles += 1
    #             users[user] = info
    #             print(info)
    #             if i > 0 and i % cache_delay == 0:
    #                 login()
    #                 _update_cache(users, file_path)
    #                 print(f'Handled batch in {time() - start} seconds')
    #                 start = time()
    #                 # print('Users cache was updated')
    #         i += 1
    #         print(f'Handled {i} / {n_total} users')
    # finally:
    #     print(f'Found {n_closed_profiles} closed profiles')
    #     _update_cache(users, file_path)
    #     # print('Users cache was updated')
