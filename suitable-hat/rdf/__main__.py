import json
import re
from itertools import chain
from urllib.parse import quote

from ..converters import line_to_triple, ANEK_TYPE_ID, USER_TYPE_ID, REMASTERING_TYPE_ID, HAS_TEXT
from ..utils import query as query_, read

ENDPOINT_IP = '10.10.0.46'
ENDPOINT_PORT = '9999'
REGEXP_PATTERN = '({uris})\\s+'
HAS_TEXT_REGEXP_PATTERN = f'({{uris}})\\s+{HAS_TEXT}\\s+'
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


def filter_triples_by_entity_frequency(triples, min_frequency: int = 2, min_anek_frequency: int = None):
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
        if entity_mentions[triple[0]] >= (min_anek_frequency if triple[0].startswith(ANEK_TYPE_ID) and min_anek_frequency is not None else min_frequency) and \
                entity_mentions[triple[2]] >= (min_anek_frequency if triple[0].startswith(ANEK_TYPE_ID) and min_anek_frequency is not None else min_frequency):
            yield triple


def query(query_path: str, triples_path: str, result_path: str, texts_path: str):
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
        filter_triples_by_entity_frequency(
            dict.fromkeys(
                get_matching_triples(
                    triples_path=triples_path,
                    regexp=re.compile(REGEXP_PATTERN.format(uris='|'.join(items)))
                )
            ), min_frequency=4, min_anek_frequency=10
        )
    )

    with open(result_path, 'w') as out:
        for triple in triples:
            out.write(' '.join(triple) + '\n')

    print("Results file is generated")

    anek_ids = tuple(
        dict.fromkeys(
            chain(
                *map(
                    lambda triple_: filter(
                        lambda id_: id_.startswith(ANEK_TYPE_ID) or id_.startswith(REMASTERING_TYPE_ID),
                        triple_
                    ),
                    triples
                )
            )
        )
    )

    with open(texts_path, 'w') as out:
        for triple in get_matching_triples(
                triples_path=triples_path,
                regexp=re.compile(HAS_TEXT_REGEXP_PATTERN.format(uris='|'.join(anek_ids)))
        ):
            out.write(f'{triple[0]} {triple[2]}' + '\n')
