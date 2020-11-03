import pickle


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


def write(filename, content):
    with open(filename, 'w') as f:
        f.write(content)
        f.flush()
