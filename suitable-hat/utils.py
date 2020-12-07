import pickle
from pdftotext import PDF
import re
from urllib.request import urlopen, Request

ABSTRACT_HEADING = 'abstract'
REFERENCES_HEADING = re.compile('references|acknowledgments')
HEADING = re.compile('[0-9]+\s+([^\s]+.+)')
NUMBER = re.compile('[0-9\.]+')
PHRASE = re.compile('[0-9\w\s]+')
SPACE = re.compile('\s+')


class Line:
    def __init__(self, line: str):
        self.line = line
        self.phrases = PHRASE.findall(line)

    @property
    def n_chars(self):
        return len(self.line)

    @property
    def n_phrases(self):
        return len(self.phrases)

    @property
    def average_phrase_length(self):
        return sum(
            len(phrase)
            for phrase in self.phrases
        ) / float(
            self.n_phrases
        ) if self.n_phrases > 0 else 0


def read_pdf(path: str):
    with open(path, "rb") as file:
        return PDF(file)


def preprocess_pdf(pdf: PDF):
    def drop_footnotes(lines: list, max_footnote_length: int = 1):
        footnote = []
        have_passed_footnotes = False
        lines_without_footnotes = []
        for line in reversed(lines):
            if have_passed_footnotes:
                lines_without_footnotes.append(line)
            else:
                if NUMBER.fullmatch(line):
                    footnote = []
                elif len(footnote) <= max_footnote_length:
                    footnote.append(line)
                else:
                    have_passed_footnotes = True
                    lines_without_footnotes.extend(footnote)
                    lines_without_footnotes.append(line)
        return reversed(lines_without_footnotes)

    def filter_lines(i: int, lines):
        have_passed_authors = False
        have_started_references = False
        for line in lines:
            if line.lower() == ABSTRACT_HEADING:
                have_passed_authors = True
                continue
            elif REFERENCES_HEADING.fullmatch(line.lower()):
                have_started_references = True
                continue
            if (have_passed_authors or i > 0) and not have_started_references:
                heading_match = HEADING.fullmatch(line)
                if heading_match is None:
                    yield line
                else:
                    yield heading_match.group(1)

    def filter_by_stats(lines):
        line_objects = [
            Line(SPACE.sub(' ', line))
            for line in lines
        ]
        mean_line_length = sum(
            line.n_chars
            for line in line_objects
        ) / float(len(line_objects)) if len(line_objects) > 0 else 0
        mean_average_phrase_length = sum(
            line.average_phrase_length
            for line in line_objects
        ) / float(len(line_objects)) if len(line_objects) > 0 else 0
        mean_n_phrases = sum(
            line.average_phrase_length
            for line in line_objects
        ) / float(len(line_objects)) if len(line_objects) > 0 else 0

        # print(mean_line_length, mean_average_phrase_length, mean_n_phrases)

        for line in line_objects:
            if not line.line.isupper() and line.average_phrase_length >= mean_average_phrase_length / 3.0:
                yield line.line
                # print(line.line, line.n_chars, line.average_phrase_length, line.n_phrases)

        # return [
        #     line.line for line in line_objects
        # ]

    for i, page in enumerate(pdf):
        lines = drop_footnotes(
            [
                line.strip()
                for line in page.split('\n')
            ]
        )
        yield '\n'.join(
            filter_by_stats(
                filter_lines(
                    i,
                    lines
                )
            )
        )


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
