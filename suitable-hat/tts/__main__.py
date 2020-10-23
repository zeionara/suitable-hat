import re


def split_text(input_text, max_length=500):
    def split_text_rec(input_text, regexps, max_length=max_length):
        if len(input_text) <= max_length:
            return [input_text]

        if isinstance(regexps, str):
            regexps = [regexps]

        regexp = regexps.pop(0) if regexps else f'(.{{{max_length}}})'

        text_list = re.split(regexp, input_text)
        combined_text = []

        combined_text.extend(split_text_rec(text_list.pop(0), regexps, max_length))
        for val in text_list:
            current = combined_text.pop()
            concat = current + val
            if len(concat) <= max_length:
                combined_text.append(concat)
            else:
                combined_text.append(current)
                combined_text.extend(split_text_rec(val, regexps, max_length))
        return combined_text

    return split_text_rec(input_text=input_text.replace('\n', ' '), regexps=['([\,|\.|;]+)', '( )'])


def pre_process(text: str):
    return text.replace('"', "'").replace('~', 'тильда').replace('/', '').replace('\\', '').replace('\t', ' ').replace('\n', ' ')


def report_error(e, url: str):
    print(f'{e} when querying url {url}')
