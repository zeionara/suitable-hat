from ...converters import line_to_triple, HAS_TEXT, ANEK_TYPE_ID, HAS_ID, RESEMBLES, REMASTERING_TYPE_ID

COMMUNITIES = (85443458, 45491419, 149279263)


def describe_existing_data(path: str, n_lines_per_log_entry: int = 1000000):
    users = set()
    remastering_counters = {}
    anek_texts = set()
    n_aneks = 0
    n_remasterings = 0
    with open(path, 'r') as file:
        for i, line in enumerate(file):
            head, relationship, tail = line_to_triple(line)
            if relationship == HAS_TEXT:
                if head.startswith(ANEK_TYPE_ID):
                    n_aneks += 1
                    if tail not in anek_texts:
                        if head not in remastering_counters:
                            remastering_counters[head] = {
                                'text': tail,
                                'n-remasterings': 0
                            }
                        else:
                            remastering_counters[head]['text'] = tail
                        anek_texts.add(tail)
                elif head.startswith(REMASTERING_TYPE_ID):
                    n_remasterings += 1
            elif relationship == HAS_ID:
                users.add(head)
            elif relationship == RESEMBLES:
                if tail not in remastering_counters:
                    remastering_counters[tail] = {
                        'text': None,
                        'n-remasterings': 1
                    }
                else:
                    remastering_counters[tail]['n-remasterings'] += 1
            if i % n_lines_per_log_entry == 0 and i > 0:
                print(f'Handled {i} lines')
    return {
        'remastering-counters': {
            anek['text']: anek['n-remasterings']
            for anek in remastering_counters.values()
            if anek['text'] is not None
        },
        'users': users,
        'n-aneks': n_aneks,
        'n-remasterings': n_remasterings
    }


def is_end_of_patch(existing_data_description: dict, aneks: dict, n_remasterings_relative_margin: int = 0.2):
    def is_edge_anek(anek: dict):
        if anek['text'] not in existing_data_description['remastering-counters']:
            return False
        old_n_remasterings = existing_data_description['remastering-counters'][anek['text']]
        new_n_remasterings = len(anek['remasterings'])
        if old_n_remasterings > 0:
            print(f'Margin is {new_n_remasterings} / {old_n_remasterings}', new_n_remasterings / old_n_remasterings - 1.0)
        return (
                old_n_remasterings == new_n_remasterings == 0 or
                (
                        old_n_remasterings > 0 and
                        new_n_remasterings / old_n_remasterings - 1.0 <= n_remasterings_relative_margin
                )
        )

    _is_end_of_patch = False
    for anek in aneks['aneks']:
        if is_edge_anek(anek) and not _is_end_of_patch:
            _is_end_of_patch = True
        elif not is_edge_anek(anek) and _is_end_of_patch:
            _is_end_of_patch = False
    return _is_end_of_patch


def drop_redundant_triples(triples: iter, existing_data_description: dict):
    raise NotImplementedError()
    # for head, relationship, tail in triples:
    #     if not (
    #             relationship == HAS_TEXT and tail in existing_data_description['remastering-counters']
    #     ):
    #         yield head, relationship, tail
