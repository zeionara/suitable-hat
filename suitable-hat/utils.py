import pickle


def read_file(path: str):
    with open(path, 'r') as f:
        return f.read()


def read_cache(cache_path: str):
    with open(cache_path, 'rb') as f:
        return pickle.load(f)
