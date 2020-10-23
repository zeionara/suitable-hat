import click

from .parsing import parse, merge
from .tts.crt import generate_audio as generate_audio_with_crt
from .tts.google import generate_audio as generate_audio_with_google


@click.group()
def main():
    pass


@main.command()
@click.option('--community-id', type=int, default=85443458)
@click.option('--min-length', type=int, default=25)
@click.option('--cache-delay', type=int, default=100)
@click.option('--cache-path', type=str, default='aneks.pkl')
@click.option('--remasterings', '-r', is_flag=True)
def load(community_id: int = 85443458, min_length: int = 25, cache_delay: int = 100, cache_path='aneks.pkl', remasterings: bool = False):
    parse(community_id=community_id, min_length=min_length, cache_delay=cache_delay, cache_path=cache_path, remasterings=remasterings)


@main.command()
@click.option('--cache-dir', type=str, default='cache')
@click.option('--file-path', type=str, default='aneks.txt')
def join(cache_dir: str = 'cache', file_path: str = 'aneks.txt'):
    merge(cache_dir, file_path)


@main.command()
@click.argument('engine', type=click.Choice(['crt', 'google']), default='crt')
@click.option('--input-file', type=str, default='text.txt')
@click.option('--output-file', type=str, default='audio.mp3')
@click.option('--after-chunk-delay', type=int, default=2)
@click.option('--after-file-delay', type=int, default=60)
@click.option('--max-n-chars', type=int, default=500)
def tts(engine: str = 'crt', input_file: str = 'text.txt', output_file: str = 'audio.mp3', after_chunk_delay: int = 2, after_file_delay: int = 60, max_n_chars: int = 500):
    if engine == 'crt':
        generate_audio_with_crt(input_file_path=input_file, output_file_path=output_file, after_chunk_delay=after_chunk_delay, after_file_delay=after_file_delay, max_n_chars=max_n_chars)
    elif engine == 'google':
        generate_audio_with_google(input_file_path=input_file, output_file_path=output_file, after_chunk_delay=after_chunk_delay, after_file_delay=after_file_delay, max_n_chars=max_n_chars)
    else:
        raise ValueError(f'Unknown engined identifier {engine}')


if __name__ == "__main__":
    main()
