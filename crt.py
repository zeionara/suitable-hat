#!/usr/bin/python

import wave
import sys
import argparse
import re
import urllib
from urllib.request import Request, URLError, urlopen
import time
from collections import namedtuple
import scipy.io.wavfile
import numpy


def split_text(input_text, max_length=500):
    """
    Try to split between sentences to avoid interruptions mid-sentence.
    Failing that, split between words.
    See split_text_rec
    """
    def split_text_rec(input_text, regexps, max_length=max_length):
        """
        Split a string into substrings which are at most max_length.
        Tries to make each substring as big as possible without exceeding
        max_length.
        Will use the first regexp in regexps to split the input into
        substrings.
        If it it impossible to make all the segments less or equal than
        max_length with a regexp then the next regexp in regexps will be used
        to split those into subsegments.
        If there are still substrings who are too big after all regexps have
        been used then the substrings, those will be split at max_length.

        Args:
            input_text: The text to split.
            regexps: A list of regexps.
                If you want the separator to be included in the substrings you
                can add parenthesis around the regular expression to create a
                group. Eg.: '[ab]' -> '([ab])'

        Returns:
            a list of strings of maximum max_length length.
        """
        if(len(input_text) <= max_length): return [input_text]

        #mistakenly passed a string instead of a list
        if isinstance(regexps, str): regexps = [regexps]
        regexp = regexps.pop(0) if regexps else '(.{%d})' % max_length

        text_list = re.split(regexp, input_text)
        combined_text = []
        #first segment could be >max_length
        combined_text.extend(split_text_rec(text_list.pop(0), regexps, max_length))
        for val in text_list:
            current = combined_text.pop()
            concat = current + val
            if(len(concat) <= max_length):
                combined_text.append(concat)
            else:
                combined_text.append(current)
                #val could be >max_length
                combined_text.extend(split_text_rec(val, regexps, max_length))
        return combined_text

    return split_text_rec(input_text.replace('\n', ' '),
                          ['([\,|\.|;]+)', '( )'])


audio_args = namedtuple('audio_args',['language','output'])

def audio_extract(input_text='',args=None):
    # This accepts :
    #   a dict,
    #   an audio_args named tuple
    #   or arg parse object
    if args is None:
        args = audio_args(language='en',output=wave.open('output.mp3', 'wb'))
    if type(args) is dict:
        args = audio_args(
                    language=args.get('language','ru'),
                    output=wave.open(args.get('output','output.mp3'), 'wb')
        )
    #process input_text into chunks
    #Google TTS only accepts up to (and including) 100 characters long texts.
    #Split the text in segments of maximum 100 characters long.
    combined_text = split_text(input_text)

    args.output = wave.open(args.output.name, 'wb')

    args.output.setnchannels(1)
    args.output.setsampwidth(2)
    args.output.setframerate(22050)

    print(type(args.output))
    #download chunks and write them to the output file
    for idx, val in enumerate(combined_text):
        val = val.replace('"', "'").replace('~', 'тильда')
        mp3url = "https://cloud.speechpro.com/api/tts/synthesize/demo"
        print(mp3url)
        headers = {
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
        body = f'{{"voice_name":"Vladimir_n","text_value":"{val}"}}'
        print(body)
        req = Request(mp3url, body.encode(), headers)
        sys.stdout.write('.')
        sys.stdout.flush()
        if len(val) > 0:
            try:
                print('k')
                response = urlopen(req)
                res = response.read()
                print(len(res))
                args.output.writeframes(res[1000:])
                # numpy_data = numpy.array(response.read(), dtype=float)
                # scipy.io.wavfile.write(args.output.name, 22000, numpy_data)
                print('k')
                time.sleep(2)
                print('l')
            except URLError as e:
                print ('%s' % e)
    args.output.close()
    time.sleep(60)
    # print('Saved MP3 to %s' % args.output.name)


def text_to_speech_mp3_argparse():
    description = 'Google TTS Downloader.'
    parser = argparse.ArgumentParser(description=description,
                                     epilog='tunnel snakes rule')
    parser.add_argument('-o', '--output',
                        action='store', nargs='?',
                        help='Filename to output audio to',
                        type=argparse.FileType('wb'), default='out.mp3')
    parser.add_argument('-l', '--language',
                        action='store',
                        nargs='?',
                        help='Language to output text to.', default='en')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--file',
                       type=argparse.FileType('r'),
                       help='File to read text from.')
    group.add_argument('-s', '--string',
                       action='store',
                       nargs='+',
                       help='A string of text to convert to speech.')
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()
    

if __name__ == "__main__":
    args = text_to_speech_mp3_argparse()
    if args.file:
        input_text = args.file.read()
    if args.string:
        input_text = ' '.join(map(str, args.string))
    audio_extract(input_text=input_text, args=args)