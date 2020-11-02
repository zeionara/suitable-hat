import re
from itertools import chain

from bs4 import BeautifulSoup

from .utils.collections import next_or_none
from .utils.queries import query, query_sequence
from .utils.string import make_community_ref

wall_post_id_regexp = re.compile('wall(?:_reply)?-[0-9]+_[0-9]+')


def get_post_id(content):
    return int(content['id'].split('_')[-1])


def get_likes(content):
    post_id = wall_post_id_regexp.findall(str(content))[0]
    return query_sequence(
        items=[],
        query_=lambda offset: query(
            url=f'https://vk.com/wkview.php?act=show&offset={offset}&al=1&dmcah=&is_znav=1&ref=&w=likes/{post_id}',
            as_vk_payload=True
        ),
        parse_=lambda bs: [
            like['href']
            for like in bs.find_all('a', {'class': 'fans_fan_lnk'})
        ]
    )


def get_remasterings(content, community_id: int):
    def parse_(data):
        return [
            {
                'author': next_or_none(
                    map(
                        lambda author: author.find_all('a', {'class': 'author'})[0]['href'],
                        reply.find_all('div', {'class': 'reply_author'})
                    )
                ),
                'text': next_or_none(
                    map(
                        lambda post: post.text,
                        reply.find_all('div', {'class': 'wall_reply_text'})
                    )
                ),
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
                    url=f'https://vk.com/wall-{community_id}?act=get_post_replies&al=1&count=20&item_id={post_id}&offset={offset}&order=smart&prev_id={post_id}'
                ),
                parse_=parse_
            )
        )
    )
    return remasterings


def get_posts(community_id: int, offset: int):
    response = query(url=f'https://vk.com/wall-{community_id}?own=1&offset={offset}')
    soup = BeautifulSoup(response, features="html.parser")
    community_ref = make_community_ref(community_id)
    aneks = [
        {
            'author': next_or_none(
                map(
                    lambda author: author.find_all('a', {'class': 'wall_signed_by'})[0]['href'],
                    content.find_all('div', {'class': 'wall_signed'})
                ),
                default=next_or_none(
                    map(
                        lambda author: author.find_all('a', {'class': 'author'})[0]['href'],
                        content.find_all('h5', {'class': 'post_author'})
                    )
                )
            ),
            'text': next_or_none(
                map(
                    lambda post: post.text,
                    content.find_all('div', {'class': 'wall_post_text'})
                )
            ),
            'community': community_ref,
            'likes': get_likes(content),
            'remasterings': get_remasterings(content, community_id=community_id)
        }
        for content in soup.find_all('div', {'class': 'post'})
    ]
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
