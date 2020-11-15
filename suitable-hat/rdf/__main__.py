import json
import os
import re
from itertools import chain
from urllib.parse import quote

from ..converters import line_to_triple, ANEK_TYPE_ID, USER_TYPE_ID
from ..utils import query as query_, read, write_cache, read_cache

ENDPOINT_IP = '10.10.0.46'
ENDPOINT_PORT = '9999'
REGEXP_PATTERN = '({uris})\\s+'
NORMALIZATION_SCRIPT_PATH = 'scripts/normalize-dataset.sh'
N_ANEKS = 5
N_USERS = 20

HEADERS = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}


def unwrap_uris(item, process: callable = None):
    for value in item.values():
        if value['type'] == 'uri':
            yield value['value'] if callable is None else process(value['value'])


def get_matching_triples(triples_path: str, regexp):
    with open(triples_path, 'r') as inp:
        for line in filter(regexp.match, inp):
            yield line_to_triple(line)

def filter_triples_by_entity_frequency(triples, min_frequency: int = 2):
    entity_mentions = {}
    for triple in triples:
        if triple[0] not in entity_mentions:
            entity_mentions[triple[0]] = 1
        else:
            entity_mentions[triple[0]] += 1
        if triple[2] not in entity_mentions:
            entity_mentions[triple[2]] = 1
        else:
            entity_mentions[triple[2]] += 1

    for triple in triples:
        if entity_mentions[triple[0]] >= min_frequency and entity_mentions[triple[2]] >= min_frequency:
            yield triple


def query(query_path: str, triples_path: str, result_path: str):
    while True:
        response = json.loads(
            query_(
                url=f'http://{ENDPOINT_IP}:{ENDPOINT_PORT}/bigdata/namespace/kb/sparql?query={quote(read(query_path))}',
                headers=HEADERS,
            )
        )

        items = set(
            chain(
                *[
                    unwrap_uris(result, process=lambda uri: uri.split('/')[-1])
                    for result in response['results']['bindings']
                ]
            )
        )

        # print(items)

        aneks = [item for item in items if item.startswith(ANEK_TYPE_ID)]
        users = [item for item in items if item.startswith(USER_TYPE_ID)]

        # print(len(aneks), len(users))

        if len(aneks) >= N_ANEKS and len(users) >= N_USERS:
            items = aneks[:N_ANEKS] + users[:N_USERS]
            break

    triples = tuple(
      get_matching_triples(
          triples_path=triples_path,
          regexp=re.compile(REGEXP_PATTERN.format(uris='|'.join(items)))
      )
    )
    #    write_cache(triples, 'cache.pkl')
    # triples = read_cache('cache.pkl')
    # print(len(triples))
    
    with open(result_path, 'w') as out:
        for triple in filter_triples_by_entity_frequency(triples, min_frequency=4):
            out.write(' '.join(triple) + '\n')

    # for triple in filtered_triples:
    #     print(triple)
    # print(len(filtered_triples))
    # with open(triples_path, 'r') as inp:
    #     with open(result_path, 'w') as out:
    #         for line in filter(regexp.match, inp):
    #             out.write(line)

    os.system(f'{NORMALIZATION_SCRIPT_PATH} {result_path}')
