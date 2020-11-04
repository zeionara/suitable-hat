import pickle
from functools import partial
from itertools import chain
from multiprocessing import Pool
from os import listdir
from os.path import isfile, join
from time import time
from typing import Tuple
from urllib.error import HTTPError

from .converters import to_triples, users_to_triples, triples_to_graph
from .parsing.aneks import get_posts
from .parsing.users import get_ids, get_friends, get_communities
from .parsing.utils.patching import describe_existing_data, is_end_of_patch
from .utils import write_cache


def parse_patch(input_file: str = 'assets/0.8.txt', output_file: str = 'assets/patch.ttl'):
    start = time()

    # Get summary of an existing dataset

    existing_data_description = describe_existing_data(input_file)

    # Perform necessary queries for obtaining new data

    # write_cache(existing_data_description, 'assets/existing-data-description.pkl')
    # existing_data_description = read_cache('assets/existing-data-description.pkl')
    aneks = parse(
        should_stop=lambda aneks_: is_end_of_patch(existing_data_description, aneks_),
        should_cache=False
    )
    # write_cache(aneks, 'assets/aneks-patch.pkl')
    # aneks = read_cache('assets/aneks-patch.pkl')
    new_users = tuple(
        set(
            map(
                lambda user: user[1:],
                aneks['users']
            )
        ).difference(existing_data_description['users'])
    )
    users = load_users(users=new_users, should_cache=False, chunk_size=20)
    # write_cache(users, 'assets/users-patch.pkl')
    # users = read_cache('assets/users-patch.pkl')

    # Convert collected data to triples

    anek_triples = to_triples(aneks=aneks, first_anek_id=existing_data_description['n-aneks'], first_remastering_id=existing_data_description['n-remasterings'])
    # print(anek_triples)
    user_triples = users_to_triples(users=users)
    # print(user_triples)

    # print(user_triples)
    # for user, _, __ in user_triples:
    #     if user in existing_data_description['users']:
    #         print(user)

    # triples = tuple(
    #     drop_redundant_triples(
    #         triples=chain(
    #             anek_triples,
    #             user_triples
    #         ),
    #         existing_data_description=existing_data_description
    #     )
    # )
    # triples = tuple(
    #     chain(
    #         anek_triples,
    #         user_triples
    #     )
    # )

    # Write triples on disk

    triples_to_graph(
        triples=chain(
            anek_triples,
            user_triples
        ),
        output_file=output_file
    )

    print(f'Generated patch in {time() - start} seconds (collected {len(anek_triples) + len(user_triples)} triples).')

    # print(is_end_of_patch(existing_data_description, aneks))
    # for i, anek in enumerate(aneks['aneks']):
    #     if anek['text'] in existing_data_description['remastering-counters']:
    #         # print(i, anek['text'])
    #         print(len(anek['remasterings']), )
    # print(existing_data_description['users'])


def parse(community_id: int = 45491419, offset: int = 0, cache_delay: int = 100, cache_path='aneks.pkl',
          should_stop: callable = lambda aneks: len(aneks['aneks']) == 0, should_cache: bool = True):
    aneks = {'aneks': [], 'users': set()}
    anek_texts = set()
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
            if anek['text'] not in anek_texts:
                anek_texts.add(anek['text'])
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


def merge(dir_path: str = 'caches', file_path: str = 'aneks.pkl'):
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
            aneks_ = pickle.load(f)
        for anek in aneks_['aneks']:
            if anek['text'] not in anek_texts:
                anek_texts.add(anek['text'])
                anek['remasterings'] = list(anek['remasterings'])
                aneks['aneks'].append(anek)
        aneks['users'] = aneks_['users'].union(aneks['users'])

    aneks['users'] = list(aneks['users'])
    write_cache(aneks, file_path)


def _handle_chunk_of_users(enumerated_chunk, chunk_size: int, output_dir: str, should_cache: bool = True):
    i, chunk = enumerated_chunk
    filename = f'{output_dir}/{i * chunk_size}-{(i + 1) * chunk_size}.pkl'
    if should_cache and isfile(filename):
        print(f'Skipping {filename}...')
        return
    ids = get_ids(chunk)
    for j, (key, value) in enumerate(ids.items()):
        if 'id' in value and not (value['is-closed'] or value['is-deleted']):
            value['communities'] = get_communities(value['id'])
            value['friends'] = get_friends(value['id'])
        print(f"{j} {key}: {value}")
    if should_cache:
        write_cache(ids, filename)
    else:
        return ids


def load_users(input_file: str = 'assets/baneks.pkl', should_test: bool = False, reverse: bool = False, chunk_size: int = 200, n_workers: int = 5, output_dir: str = 'assets/users',
               users: Tuple = None, should_cache: bool = False):
    if users is None:
        if should_test:
            users = [
                '/mcluvin', '/gospes', '/agrishin', '/sergortm', '/lexajeas', '/id10562339', '/ksp256', '/poluektoff_08', '/id250088249', '/id57486417', '/katherinaklim', '/kateeeeeriina',
                '/mrbloodness', '/dimchu', '/idinaxyi31', '/id_hell_paradise', '/sugarhl', '/pinkamena', '/id83420082', '/id556218588', '/idrecon666', '/valera_popov_0704', '/oimygoddaddy',
                '/id210257337', '/77mike', '/id16449012', '/igor_z93', '/id161114435', '/msirkin', '/jon_pelegrim', '/id117934156', '/dlnkayt', '/fineguy', '/megamrazhoma', '/faerrx',
                '/valera.gorbunov', '/id32572564', '/battsn5', '/dimka26', '/noirshinobi', '/id500133915', '/pushkinalove', '/id192746934', '/morgol07', '/dlukich', '/ipezio', '/alkoforce',
                '/id106100939', '/id317006682', '/phephe975', '/akoretsk', '/id43721345', '/id270101922', '/id143522890', '/antusheva03', '/vovan7590', '/phantom87', '/id600994201', '/docm0t',
                '/doom04', '/id20929669', '/valentln1337', '/e_k_xrystik', '/ares3', '/id175277384', '/gggqggge', '/robbievil', '/andrey_b1999', '/fox_martin', '/kartavii_hren', '/daria_sakyra',
                '/id303227458', '/merkulow', '/yudinko', '/ad_with_ak', '/alya_dyra', '/id288088296', '/id14840489', '/alex_goldenmyer', '/chaechka_e', '/id3644923', '/drevniydraianec',
                '/ks.evtushenko', '/n.bewiga', '/naggets16', '/tea_sweet_t', '/drakosha_d', '/id563245194', '/skyinmymind', '/id188720379', '/id142901702', '/id138075134', '/whoa_m1',
                '/gordina1707', '/qucro', '/justrooit', '/id183295771', '/marsh_stanley_marsh', '/vanessa_super', '/anomalechka'
            ]
        else:
            with open(input_file, 'rb') as f:
                aneks = pickle.load(f)
            users = aneks['users']
            del aneks

    n_chunks = len(users) // chunk_size + 1
    start = time()
    with Pool(n_workers) as pool:
        result = pool.map(
            partial(_handle_chunk_of_users, chunk_size=chunk_size, output_dir=output_dir, should_cache=should_cache),
            [(i, users[i * chunk_size:(i + 1) * chunk_size]) for i in (reversed(range(n_chunks)) if reverse else range(n_chunks))]
        )
    print(f'Completed in {time() - start} seconds')
    if not should_cache:
        return {
            user: user_info
            for batch in result
            for user, user_info in batch.items()
        }
