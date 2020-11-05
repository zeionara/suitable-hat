import re
from functools import partial
from itertools import chain
from multiprocessing.pool import ThreadPool

from bs4 import BeautifulSoup

from .utils.collections import next_or_none
from .utils.queries import query, query_sequence
from .utils.string import make_community_ref

wall_post_id_regexp = re.compile('wall(?:_reply)?-[0-9]+_[0-9]+')


def get_html_post_id(content):
    return wall_post_id_regexp.findall(str(content))[0]


def get_post_id(content):
    return int(content['id'].split('_')[-1])


def get_reply_id(content):
    html_id = get_html_post_id(content)
    return int(html_id.split('_')[-1])


def get_likes(content):
    post_id = get_html_post_id(content)
    return query_sequence(
        items=[],
        query_=lambda offset: query(
            url=f'https://vk.com/wkview.php?act=show&offset={offset}&al=1&dmcah=&is_znav=1&ref=&w=likes/{post_id}',
            as_vk_payload=True
        ),
        parse_=lambda bs: [
            int(like['data-id'])
            for like in bs.find_all('div', {'class': 'fans_fan_row'})
        ]
    )


def get_remastering_author(content):
    author = next_or_none(
        map(
            lambda author: int(author.find_all('a', {'class': 'author'})[0]['data-from-id']),
            content.find_all('div', {'class': 'reply_author'})
        )
    )
    return author


def get_remasterings(content, community_id: int):
    def parse_(data):
        return [
            {
                'author': get_remastering_author(reply),
                'text': next_or_none(
                    map(
                        lambda post: post.text,
                        reply.find_all('div', {'class': 'wall_reply_text'})
                    )
                ),
                'id': get_reply_id(reply),
                'community': community_ref,
                'likes': get_likes(reply)
            } for reply in data.find_all('div', {'class': 'reply_content'})
        ]

    community_ref = make_community_ref(community_id)
    post_id = get_post_id(content)
    remasterings = tuple(
        filter(
            lambda reply: reply['author'] is not None and reply['text'] is not None,
            query_sequence(
                items=parse_(content),
                query_=lambda offset: query(
                    url=f'https://vk.com/wall-{community_id}?act=get_post_replies&al=1&count=20&item_id={post_id}&offset={offset}&order=smart&prev_id={post_id}',
                    as_vk_payload=True
                ),
                parse_=parse_
            )
        )
    )
    return remasterings


def get_post_author(content):
    author = next_or_none(
        map(
            lambda author_: int(author_.find_all('a', {'class': 'wall_signed_by'})[0]['mention_id'][2:]),
            content.find_all('div', {'class': 'wall_signed'})
        )
        # default={
        #     'id': next_or_none(
        #         map(
        #             lambda author: author.find_all('a', {'class': 'author'})[0]['href'][1:],
        #             content.find_all('h5', {'class': 'post_author'})
        #         )
        #     ),
        #     'is-user': False
        # }
    )
    return author


def _handle_post(content, community_ref: str, community_id: int):
    return {
        'author': get_post_author(content),
        'text': next_or_none(
            map(
                lambda post: post.text,
                content.find_all('div', {'class': 'wall_post_text'})
            )
        ),
        'community': community_ref,
        'likes': get_likes(content),
        'id': get_post_id(content),
        'remasterings': get_remasterings(content, community_id=community_id)
    }


def get_posts(community_id: int, offset: int):
    response = query(url=f'https://vk.com/wall-{community_id}?own=1&offset={offset}')
    soup = BeautifulSoup(response, features="html.parser")
    community_ref = make_community_ref(community_id)

    aneks_to_handle = soup.find_all('div', {'class': 'post'})
    if len(aneks_to_handle) > 0:
        with ThreadPool(len(aneks_to_handle)) as pool:
            aneks = pool.map(
                partial(_handle_post, community_ref=community_ref, community_id=community_id),
                aneks_to_handle
            )
    else:
        aneks = ()

    return {
        'aneks': aneks,
        'users': list(
            set(
                chain(
                    map(
                        lambda item: item['author'],
                        aneks
                    ),
                    *map(
                        lambda item: map(
                            lambda remastering: remastering['author'],
                            item['remasterings']
                        ), aneks
                    ),
                    *map(
                        lambda item: item['likes'],
                        aneks
                    ),
                    *map(
                        lambda item: chain(
                            *map(
                                lambda remastering: remastering['likes'],
                                item['remasterings']
                            )
                        ), aneks
                    )
                )
            )
        )
    }
