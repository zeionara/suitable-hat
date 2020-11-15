import os
import pickle
from functools import partial
from itertools import chain
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from os import listdir
from os.path import isfile, join
from time import time
from typing import Tuple
from urllib.error import HTTPError

from .converters import to_triples, users_to_triples, _write_triple, triples_to_graph
from .parsing.aneks import get_posts
from .parsing.users import get_friends, get_communities
from .parsing.utils.patching import is_end_of_patch, COMMUNITIES, describe_existing_data
from .utils import write_cache


def _parse_community_aneks(community_id: int, existing_data_description: dict):
    return parse(
        community_id=community_id,
        should_stop=lambda aneks_: is_end_of_patch(existing_data_description, aneks_),
        should_cache=False
    )


def parse_patch(input_file: str = 'assets/0.8.txt', output_file: str = 'assets/patch.ttl'):
    start = time()

    # Get summary of an existing dataset

    existing_data_description = describe_existing_data(input_file)

    # Perform necessary queries for obtaining new data

    with Pool(len(COMMUNITIES)) as pool:
        aneks = merge(
            aneks_groups=pool.map(
                partial(_parse_community_aneks, existing_data_description=existing_data_description),
                COMMUNITIES
            )
        )

    users = load_users(
        users=tuple(
            set(
                map(
                    lambda user: user[1:],
                    aneks['users']
                )
            ).difference(existing_data_description['users'])
        ), should_cache=False, chunk_size=20
    )

    # Convert collected data to triples

    anek_triples = to_triples(aneks=aneks)
    user_triples = users_to_triples(users=users)

    # Write triples on disk

    triples = chain(
        anek_triples,
        user_triples
    )
    if output_file.split('.')[-1] == 'ttl':
        triples_to_graph(
            triples=chain(
                anek_triples,
                user_triples
            ),
            output_file=output_file
        )
    else:
        with open(output_file, 'w') as file:
            for triple in triples:
                _write_triple(file, triple)

    print(f'Generated patch in {time() - start} seconds (collected {len(anek_triples) + len(user_triples)} triples).')


def parse_all(output_file: str = 'aneks.pkl'):
    with Pool(len(COMMUNITIES)) as pool:
        aneks = merge(
            aneks_groups=pool.map(
                parse,
                COMMUNITIES
            )
        )
    write_cache(aneks, output_file)


def parse(community_id: int = 45491419, offset: int = 0, cache_delay: int = 100, cache_path=None,
          should_stop: callable = lambda aneks: len(aneks['aneks']) == 0, should_cache: bool = True):
    if cache_path is None:
        cache_path = f'assets/{community_id}.pkl'
    aneks = {'aneks': [], 'users': set()}
    anek_ids = set()
    while True:
        print(f'Offset = {offset}')
        try:
            aneks_ = get_posts(community_id, offset)
        except HTTPError:
            if should_cache:
                write_cache(aneks, cache_path)
            raise
        offset += len(aneks_['aneks'])
        for anek in filter(
                lambda anek_: anek_['text'] is not None,
                aneks_['aneks']
        ):
            if anek['id'] not in anek_ids:
                anek_ids.add(anek['id'])
                aneks['aneks'].append(anek)
        aneks['users'] = set(aneks_['users']).union(aneks['users'])
        if should_cache and (offset // cache_delay - (offset - len(aneks_['aneks'])) // cache_delay) > 0:
            write_cache(aneks, cache_path)
        print(aneks_)
        print(f'Handled {offset} (+{len(aneks_["aneks"])}) aneks')
        if should_stop(aneks_):
            break
    if should_cache:
        write_cache(aneks, cache_path)
    return aneks


def merge(dir_path: str = None, file_path: str = None, aneks_groups: iter = None):
    assert not (dir_path and aneks_groups) and (dir_path or aneks_groups)

    def append_aneks(aneks__: dict):
        for anek in aneks__['aneks']:
            anek_id = (anek['community'], anek['id'])
            if anek_id not in anek_ids:
                anek_ids.add(anek_id)
                anek['remasterings'] = list(anek['remasterings'])
                aneks['aneks'].append(anek)
        aneks['users'] = aneks__['users'].union(aneks['users'])

    aneks = {'aneks': [], 'users': set()}
    anek_ids = set()
    if aneks_groups is None:
        for file in filter(
                lambda file_path_: isfile(file_path_),
                map(
                    lambda file_path_: join(dir_path, file_path_),
                    listdir(dir_path)
                )
        ):
            with open(file, 'rb') as f:
                append_aneks(pickle.load(f))
    else:
        for group in aneks_groups:
            append_aneks(group)

    aneks['users'] = list(aneks['users'])
    if file_path is not None:
        write_cache(aneks, file_path)

    return aneks


def _handle_user(user_id):
    value = {
        'communities': get_communities(user_id),
        'friends': get_friends(user_id)
    }
    print(f"{user_id}: {value}")
    return user_id, value


def _handle_chunk_of_users(enumerated_chunk, chunk_size: int, output_dir: str, should_cache: bool = True):
    i, chunk = enumerated_chunk
    chunk = [user for user in chunk if user is not None]
    filename = f'{output_dir}/{i * chunk_size}-{(i + 1) * chunk_size}.pkl'
    if should_cache and isfile(filename):
        print(f'Skipping {filename}...')
        return
    # ids = get_ids(chunk)
    n_items = len(chunk)

    if n_items > 0:
        with ThreadPool(n_items) as pool:
            ids = pool.map(
                _handle_user,
                chunk
            )
    else:
        ids = ()

    # for j, (key, value) in enumerate(ids.items()):
    #     if 'id' in value and not (value['is-closed'] or value['is-deleted']):
    #         value['communities'] = get_communities(value['id'])
    #         value['friends'] = get_friends(value['id'])
    #     print(f"{j} {key}: {value}")
    if should_cache:
        write_cache(ids, filename)
    return ids


def load_users(input_file: str = 'assets/baneks.pkl', should_test: bool = False, reverse: bool = False, chunk_size: int = 200, n_workers: int = 5, output_dir: str = 'assets/users',
               users: Tuple = None, should_cache: bool = True):
    if users is None:
        if should_test:
            users = [
                86842338, 190950887, 40077913, 224190963, 134296075, 183295771, 71955701, 288088296, 563245194, 356410583, 280817019, 142901702, 14810486, 431459795, 369068104, 210986842, 138075134,
                73261972, 175277384, 188720379, 230623608, 224881177, 345361806, 595514360, 42704353, 225958714, 14840489, 198173201, 499996054, 490365698, 15908362, 311086986, 132839076, 303227458,
                138548298, 144376886, 137561766, 3644923, 158569800, 500133915, 276656239, 234780436, 533827295, 43721345, 279763544, 20929669, 600994201, 106100939, 213148944, 35193453, 475695851,
                518132713, 38601136, 143522890, 222092628, 96042672, 222688320, 64251011, 270101922, 296044582, 32554036, 192330600, 4745539, 32957486, 257099862, 214375440, 147897952, 83420082,
                204884400, 276225935, 16449012, 117934156, 434968590, 117726871, 4855767, 135150017, 32572564, 269570072, 11203320, 161114435, 348396463, 210257337, 101751, 10562339, 489423365,
                147386027, 3050046, 42909834, 322614602, 57486417, 277047, 556218588, 226149873, 121825081, 69683935, 225152880, 13946947, 250088249, 13946947, 250088249
            ]
        else:
            with open(input_file, 'rb') as f:
                aneks = pickle.load(f)
            users = aneks['users']
            del aneks

    os.makedirs(output_dir, exist_ok=True)
    n_chunks = len(users) // chunk_size + 1
    start = time()
    with Pool(n_workers) as pool:
        result = pool.map(
            partial(_handle_chunk_of_users, chunk_size=chunk_size, output_dir=output_dir, should_cache=should_cache),
            [(i, users[i * chunk_size:(i + 1) * chunk_size]) for i in (reversed(range(n_chunks)) if reverse else range(n_chunks))]
        )
    print(f'Completed in {time() - start} seconds')
    return {
        user: user_info
        for batch in result
        if batch is not None
        for user, user_info in batch
    }
