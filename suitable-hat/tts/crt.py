import time
import wave
from urllib.error import URLError
from urllib.request import Request, urlopen

from .__main__ import split_text, report_error
from ..utils import read_file

HEADERS = {
    'Host': 'cloud.speechpro.com',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    'Content-Type': 'application/json',
    'Origin': 'https://cloud.speechpro.com',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://cloud.speechpro.com/service/tts',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7',
}


def generate_audio(input_file_path: str, output_file_path: str, voice_name: str = 'Vladimir_n', after_chunk_delay: int = 2, after_file_delay: int = 60, max_n_chars: int = 500):
    input_text = read_file(input_file_path)
    combined_text = split_text(input_text, max_length=max_n_chars)

    with wave.open(output_file_path, 'wb') as output_file_handler:
        output_file_handler.setnchannels(1)
        output_file_handler.setsampwidth(2)
        output_file_handler.setframerate(22050)

        for idx, val in enumerate(combined_text):
            val = val.replace('"', "'").replace('~', 'тильда').replace('/', '').replace('\\', '').replace('\t', ' ').replace('\n', ' ')
            mp3url = "https://cloud.speechpro.com/api/tts/synthesize/demo"
            print(mp3url)
            body = f'{{"voice_name":"{voice_name}","text_value":"{val}"}}'
            req = Request(mp3url, body.encode(), HEADERS)
            if len(val) > 0:
                try:
                    response = urlopen(req).read()
                    output_file_handler.writeframes(response[1000:])
                except URLError as e:
                    report_error(e, mp3url)
                time.sleep(after_chunk_delay)
    time.sleep(after_file_delay)
