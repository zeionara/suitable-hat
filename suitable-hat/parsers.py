import pickle
from functools import partial
from multiprocessing import Pool
from os import listdir
from os.path import isfile, join
from time import time
from urllib.error import HTTPError

from .parsing.aneks import get_posts
from .parsing.users import get_ids, get_friends, get_communities
from .utils import write_cache


def parse(community_id: int = 85443458, offset: int = 0, cache_delay: int = 100, cache_path='aneks.pkl'):
    aneks = {'aneks': [], 'users': set()}
    anek_texts = set()
    while True:
        print(f'Offset = {offset}')
        try:
            aneks_ = get_posts(community_id, offset)
        except HTTPError:
            write_cache(aneks, cache_path)
            raise
        if len(aneks_['aneks']) == 0:
            break
        offset += len(aneks_['aneks'])
        for anek in filter(
                lambda anek_: anek_['text'] is not None,
                aneks_['aneks']
        ):
            if anek['text'] not in anek_texts:
                anek_texts.add(anek['text'])
                aneks['aneks'].append(anek)
        aneks['users'] = aneks_['users'].union(aneks['users'])
        if (offset // cache_delay - (offset - len(aneks_['aneks'])) // cache_delay) > 0:
            write_cache(aneks, cache_path)
        print(aneks_)
        print(f'Handled {offset} (+{len(aneks_["aneks"])}) aneks')
    write_cache(aneks, cache_path)


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


def _handle_chunk_of_users(enumerated_chunk, chunk_size: int, output_dir: str):
    i, chunk = enumerated_chunk
    filename = f'{output_dir}/{i * chunk_size}-{(i + 1) * chunk_size}.pkl'
    if isfile(filename):
        print(f'Skipping {filename}...')
        return
    ids = get_ids(chunk)
    for j, (key, value) in enumerate(ids.items()):
        if 'id' in value and not (value['is-closed'] or value['is-deleted']):
            value['communities'] = get_communities(value['id'])
            value['friends'] = get_friends(value['id'])
        print(f"{j} {key}: {value}")
    write_cache(ids, filename)


def load_users(input_file: str = 'assets/baneks.pkl', should_test: bool = False, reverse: bool = False, chunk_size: int = 200, n_workers: int = 5, output_dir: str = 'assets/users'):
    if should_test:
        users_ = [
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
        users_ = aneks['users']
        del aneks

    n_chunks = len(users_) // chunk_size + 1
    start = time()
    with Pool(n_workers) as pool:
        pool.map(
            partial(_handle_chunk_of_users, chunk_size=chunk_size, output_dir=output_dir),
            [(i, users_[i * chunk_size:(i + 1) * chunk_size]) for i in (reversed(range(n_chunks)) if reverse else range(n_chunks))]
        )
    print(f'Completed in {time() - start} seconds')
