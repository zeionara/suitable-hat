import os
import re
from os import listdir
from os.path import join, isfile
from time import time

from rdflib import Namespace, Literal, Graph, URIRef

from .utils import read_cache, write

communities = {
    '/wall-85443458': 'baneksbest',
    '/wall-45491419': 'baneks',
    '/wall-149279263': 'anekdotikategoriib'
}

# users props
KNOWS = 'knows'
FOLLOWS = 'follows'
HAS_ID = 'has-id'
LIKED = 'liked'

# aneks props
REMASTERING_TYPE_ID = 'remastering'
RESEMBLES = 'resembles'
ANEK_TYPE_ID = 'anek'
HAS_TEXT = 'has-text'
CREATED = 'created'
PUBLISHED = 'published'

# ttl prefixes
BANEKS = Namespace('http://baneks.ru#')
RDF = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')

# ttl types
ANEK = BANEKS.Anek
REMASTERING = BANEKS.Remastering
USER = BANEKS.User
COMMUNITY = BANEKS.Community

# ttl props
HAS_TEXT_ = BANEKS.hasText
LIKES_ = BANEKS.likes
TYPE = RDF.type
HAS_ID_ = BANEKS.hasId
PUBLISHED_ = BANEKS.published
RESEMBLES_ = BANEKS.resembles
KNOWS_ = BANEKS.knows
FOLLOWS_ = BANEKS.follows
CREATED_ = BANEKS.created

# ttl uris
NODE_ = 'http://baneks.ru/{id}'

PREFIX_REGEXP = re.compile('@prefix.+\\.\n')


def _generate_triples(item: dict, item_id: str):
    yield item_id, HAS_TEXT, item['text']
    yield item['author'][1:], CREATED, item_id
    yield communities[item['community']], PUBLISHED, item_id
    for like in item['likes']:
        yield like[1:], LIKED, item_id


def _write_triple(file, triple):
    file.write(f'{" ".join(triple)}\n')


def to_triples(input_file: str = 'assets/baneks.pkl', output_file: str = 'assets/triples.txt'):
    aneks = read_cache(input_file)
    j = 0
    with open(output_file, 'w') as file:
        for i, anek in tuple(enumerate(aneks['aneks'])):
            anek_id = f'{ANEK_TYPE_ID}-{i:06d}'
            for triple in _generate_triples(anek, anek_id):
                _write_triple(file, triple)
            for remastering in anek['remasterings']:
                remastering_id = f'{REMASTERING_TYPE_ID}-{j:06d}'
                for triple in _generate_triples(remastering, remastering_id):
                    _write_triple(file, triple)
                _write_triple(file, (remastering_id, RESEMBLES, anek_id))
                j += 1


def _generate_user_triples(username: str, data: dict):
    if 'id' in data:
        yield username, HAS_ID, data['id']
    if not data['is-closed'] and not data['is-deleted']:
        for friend in data['friends']:
            yield username, KNOWS, friend
        for community in data['communities']:
            yield username, FOLLOWS, community


def users_to_triples(input_dir: str = 'assets/users', output_file: str = 'assets/triples.txt'):
    with open(output_file, 'w') as out:
        for file in filter(
                lambda file_path_: isfile(file_path_),
                map(
                    lambda file_path_: join(input_dir, file_path_),
                    listdir(input_dir)
                )
        ):
            for username, data in read_cache(file).items():
                for triple in _generate_user_triples(username, data):
                    _write_triple(out, map(str, triple))


def is_empty(graph):
    try:
        next(graph.objects())
        return False
    except StopIteration:
        return True


def triples_to_graph(input_file: str = 'assets/data.txt', output_file: str = 'assets/data.ttl', n_triples_per_graph: int = 10e6, n_triples_per_log_entry: int = 3 * 10e5):
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('baneks', BANEKS)
    i = 0
    were_prefixes_written = False

    def flush_graph():
        nonlocal graph, were_prefixes_written
        serialized_graph = graph.serialize(format='ttl').decode('utf-8')
        if were_prefixes_written:
            serialized_graph = PREFIX_REGEXP.sub('', serialized_graph)
        else:
            were_prefixes_written = True
        write(output_file, serialized_graph, should_append=True)
        graph = Graph()

    try:
        os.remove(output_file)
    except OSError:
        pass

    start = time()
    with open(input_file, 'r') as file:
        while True:
            try:
                line = file.readline()
                head, relationship, tail = line[:-1].split(' ', maxsplit=2)
                if relationship == HAS_TEXT:
                    node = URIRef(NODE_.format(id=head))
                    graph.add((node, HAS_TEXT_, Literal(tail)))
                    graph.add((node, TYPE, ANEK if head.startswith(ANEK_TYPE_ID) else REMASTERING))
                elif relationship == LIKED:
                    graph.add((URIRef(NODE_.format(id=head)), LIKES_, URIRef(NODE_.format(id=tail))))
                elif relationship == HAS_ID:
                    node = URIRef(NODE_.format(id=head))
                    graph.add((node, HAS_ID_, Literal(tail)))
                    graph.add((node, TYPE, USER))
                elif relationship == PUBLISHED:
                    node = URIRef(NODE_.format(id=head))
                    graph.add((node, TYPE, COMMUNITY))
                    graph.add((node, PUBLISHED_, URIRef(NODE_.format(id=tail))))
                elif relationship == RESEMBLES:
                    graph.add((URIRef(NODE_.format(id=head)), RESEMBLES_, URIRef(NODE_.format(id=tail))))
                elif relationship == KNOWS:
                    graph.add((URIRef(NODE_.format(id=head)), KNOWS_, URIRef(NODE_.format(id=tail))))
                elif relationship == FOLLOWS:
                    node = URIRef(NODE_.format(id=tail))
                    graph.add((node, TYPE, COMMUNITY))
                    graph.add((URIRef(NODE_.format(id=head)), FOLLOWS_, node))
                elif relationship == CREATED:
                    graph.add((URIRef(NODE_.format(id=head)), CREATED_, URIRef(NODE_.format(id=tail))))
                else:
                    raise TypeError(f'Unknown type of triple: {relationship}')
                i += 1
                if i % n_triples_per_graph == 0:
                    flush_graph()
                if i % n_triples_per_log_entry == 0:
                    print(f'Handled {i} triples after {time() - start} seconds')
                    start = time()
            except ValueError:
                break
    if not is_empty(graph):
        flush_graph()
