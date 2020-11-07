import pickle
from urllib.request import urlopen, Request


def read_file(path: str):
    with open(path, 'r') as f:
        return f.read()


def read_cache(cache_path: str):
    with open(cache_path, 'rb') as f:
        return pickle.load(f)


def write_cache(payload, cache_path: str):
    with open(cache_path, 'wb') as f:
        pickle.dump(payload, f)
    print('Cache was updated successfully')


def write(filename, content, should_append: bool = False):
    with open(filename, 'a' if should_append else 'w') as f:
        f.write(content)
        f.flush()


def read(filename):
    with open(filename, 'r') as f:
        return f.read()


def query(url: str, headers: dict, data: str = None):
    return urlopen(
        Request(
            url=url,
            headers=headers,
            data=None if data is None else data.encode()
        )
    ).read().decode(encoding='windows-1251', errors='ignore')
