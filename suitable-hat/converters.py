from .utils import read_cache

communities = {
    '/wall-85443458': 'baneksbest',
    '/wall-45491419': 'baneks',
    '/wall-149279263': 'anekdotikategoriib'
}


def _generate_triples(item: dict, item_id: str):
    yield item_id, 'has-text', item['text']
    yield item['author'][1:], 'created', item_id
    yield communities[item['community']], 'published', item_id
    for like in item['likes']:
        yield like[1:], 'liked', item_id


def _write_triple(file, triple):
    file.write(f'{" ".join(triple)}\n')


def to_triples(input_file: str = 'assets/baneks.pkl', output_file: str = 'assets/triples.txt'):
    aneks = read_cache(input_file)
    j = 0
    with open(output_file, 'w') as file:
        for i, anek in tuple(enumerate(aneks['aneks'])):
            anek_id = f'anek-{i:06d}'
            for triple in _generate_triples(anek, anek_id):
                _write_triple(file, triple)
            for remastering in anek['remasterings']:
                remastering_id = f'remastering-{j:06d}'
                for triple in _generate_triples(remastering, remastering_id):
                    _write_triple(file, triple)
                _write_triple(file, (remastering_id, 'resembles', anek_id))
                j += 1
