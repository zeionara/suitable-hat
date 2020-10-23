import click

from .parsing import parse, merge


@click.group()
def main():
    pass


@main.command()
@click.argument('community-id', type=int, default=85443458)
@click.argument('min-length', type=int, default=25)
@click.argument('cache-delay', type=int, default=100)
@click.argument('cache-path', type=str, default='aneks.pkl')
@click.option('--remasterings', '-r', is_flag=True)
def load(community_id: int = 85443458, min_length: int = 25, cache_delay: int = 100, cache_path='aneks.pkl', remasterings: bool = False):
    parse(community_id=community_id, min_length=min_length, cache_delay=cache_delay, cache_path=cache_path)


@main.command()
@click.argument('cache-dir', type=str, default='cache')
@click.argument('file-path', type=str, default='aneks.txt')
def join(cache_dir: str = 'cache', file_path: str = 'aneks.txt'):
    merge(cache_dir, file_path)


if __name__ == "__main__":
    main()
