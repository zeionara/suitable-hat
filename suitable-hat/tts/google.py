import time
import urllib
from urllib.error import URLError
from urllib.request import Request, urlopen

from .__main__ import split_text, pre_process, report_error
from ..utils import read_file, read_pdf, preprocess_pdf

HEADERS = {
    'authority': 'translate.google.com',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'x-client-data': 'CIW2yQEIprbJAQipncoBCJesygEIhrXKAQiZtcoBCPXHygE=',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7'
}


def generate_audio(input_file_path: str, output_file_path: str, language: str = 'en-US', after_chunk_delay: int = 1, after_file_delay: int = 0, max_n_chars: int = 100):

    extension = input_file_path.split('.')[-1]

    if extension == 'txt':
        input_text = read_file(input_file_path)
    else:
        input_text = '\n'.join(
            preprocess_pdf(
                read_pdf(
                    input_file_path
                )
            )
        )

    combined_text = split_text(input_text, max_length=max_n_chars)

    with open(output_file_path, 'wb') as output_file_handler:

        for idx, val in enumerate(combined_text):
            val = pre_process(val)
            mp3url = f"https://translate.google.com/translate_tts?tl={language}&client=tw-ob&q={urllib.parse.quote(val)}"
            print(f'Querying {mp3url}')
            req = Request(mp3url, None, HEADERS)
            print('Got response')
            if len(val) > 0:
                try:
                    response = urlopen(req)
                    output_file_handler.write(response.read())
                except URLError as e:
                    report_error(e, mp3url)
                time.sleep(after_chunk_delay)
    time.sleep(after_file_delay)
