import click

from .converters import to_triples as to_triples_, users_to_triples as users_to_triples_, triples_to_graph as triples_to_graph_
from .parsers import parse, merge, load_users as load_users_, parse_patch, parse_all, parse_posts
from .rdf import query as query_
from .tts.crt import generate_audio as generate_audio_with_crt
from .tts.google import generate_audio as generate_audio_with_google
from .utils import read_cache, write


@click.group()
def main():
    pass


@main.command()
@click.option('--community-id', type=int, default=85443458)
@click.option('--offset', type=int, default=0)
@click.option('--cache-delay', type=int, default=100)
@click.option('--cache-path', type=str, default='aneks.pkl')
def load(community_id: int = 85443458, offset: int = 0, cache_delay: int = 100, cache_path='aneks.pkl'):
    parse(community_id=community_id, cache_delay=cache_delay, cache_path=cache_path, offset=offset)


@main.command()
@click.option('--community-id', type=int, default=85443458)
def load_posts(community_id: int = 85443458):
    parse_posts(community_id=community_id)


@main.command()
@click.argument('path', type=str)
def split_posts(path: str):
    posts = [post for post in read_cache(cache_path=path) if post is not None and len(post) > 100]
    for i, post in enumerate(posts):
        write(f'repellent/{i:03d}.txt', post)


@main.command()
@click.option('--output-file', type=str, default='assets/aneks.pkl')
def load_all(output_file='assets/aneks.pkl'):
    parse_all(output_file=output_file)


@main.command()
@click.option('--input-file', type=str, default='assets/0.8.txt')
@click.option('--output-file', type=str, default='assets/patch.ttl')
def load_patch(input_file: str = 'assets/0.8.txt', output_file: str = 'assets/patch.ttl'):
    parse_patch(input_file=input_file, output_file=output_file)


@main.command()
@click.option('--cache-dir', type=str, default='cache')
@click.option('--file-path', type=str, default='aneks.yml')
def join(cache_dir: str = 'cache', file_path: str = 'aneks.yml'):
    merge(cache_dir, file_path)


@main.command()
@click.argument('engine', type=click.Choice(['crt', 'google']), default='crt')
@click.option('--input-file', type=str, default='text.txt')
@click.option('--output-file', type=str, default='audio.mp3')
@click.option('--after-chunk-delay', type=int, default=2)
@click.option('--language', type=str, default='en-US')
@click.option('--after-file-delay', type=int, default=60)
@click.option('--max-n-chars', type=int, default=500)
def tts(engine: str = 'crt', input_file: str = 'text.txt', output_file: str = 'audio.mp3', after_chunk_delay: int = 2, after_file_delay: int = 60, max_n_chars: int = 500, language: str = 'en-US'):
    if engine == 'crt':
        generate_audio_with_crt(input_file_path=input_file, output_file_path=output_file, after_chunk_delay=after_chunk_delay, after_file_delay=after_file_delay, max_n_chars=max_n_chars)
    elif engine == 'google':
        generate_audio_with_google(input_file_path=input_file, output_file_path=output_file, after_chunk_delay=after_chunk_delay, after_file_delay=after_file_delay, max_n_chars=max_n_chars,
                                   language=language)
    else:
        raise ValueError(f'Unknown engined identifier {engine}')


@main.command()
@click.option('--input-file', type=str, default='assets/baneks.pkl')
@click.option('--output-dir', type=str, default='assets/users')
@click.option('--should-test', is_flag=True)
@click.option('--reverse', is_flag=True)
@click.option('--chunk-size', type=int, default=200)
@click.option('--n-workers', type=int, default=5)
def load_users(input_file: str = 'assets/baneks.pkl', output_dir: str = 'assets/users', should_test: bool = False, chunk_size: int = 200, reverse: bool = False, n_workers: int = 5):
    load_users_(input_file, should_test=should_test, output_dir=output_dir, chunk_size=chunk_size, reverse=reverse, n_workers=n_workers)


@main.command()
@click.option('--input-file', type=str, default='assets/baneks.pkl')
@click.option('--output-file', type=str, default='assets/triples.txt')
def to_triples(input_file: str = 'assets/baneks.pkl', output_file: str = 'assets/triples.txt'):
    to_triples_(input_file=input_file, output_file=output_file)


@main.command()
@click.option('--input-dir', type=str, default='assets/users')
@click.option('--output-file', type=str, default='assets/triples.txt')
def users_to_triples(input_dir: str = 'assets/users', output_file: str = 'assets/triples.txt'):
    users_to_triples_(input_dir=input_dir, output_file=output_file)


@main.command()
@click.option('--input-file', type=str, default='assets/data.txt')
@click.option('--output-file', type=str, default='assets/data.ttl')
@click.option('--n-triples-per-graph', type=int, default=10e6)
@click.option('--n-triples-per-log-entry', type=int, default=3 * 10e5)
def triples_to_graph(input_file: str = 'assets/data.txt', output_file: str = 'assets/data.ttl', n_triples_per_graph: int = 10e6, n_triples_per_log_entry: int = 3 * 10e5):
    triples_to_graph_(input_file=input_file, output_file=output_file, n_triples_per_graph=n_triples_per_graph, n_triples_per_log_entry=n_triples_per_log_entry)


@main.command()
@click.option('--query-path', type=str)
@click.option('--triples-path', type=str)
@click.option('--result-path', type=str)
@click.option('--texts-path', type=str)
def query(query_path: str, triples_path: str, result_path: str, texts_path: str):
    query_(query_path=query_path, triples_path=triples_path, result_path=result_path, texts_path=texts_path)


if __name__ == "__main__":
    main()
