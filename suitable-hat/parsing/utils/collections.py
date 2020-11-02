def next_or_none(items, default=None):
    try:
        return next(items)
    except StopIteration:
        return default
