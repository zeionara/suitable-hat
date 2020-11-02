import os

from bs4 import BeautifulSoup

from .utils.queries import query


def get_ids(hrefs: iter):
    response = query(
        url=f"https://api.vk.com/method/users.get?user_ids={','.join(href[1:] for href in hrefs)}&v=5.124&fields=id,screen_name&access_token={os.environ['VK_TOKEN']}",
        as_json=True
    )

    ids = {
        item['screen_name']: {'id': item['id'], 'is-closed': item['is_closed'], 'is-deleted': False}
        for item in response['response']
        if item['first_name'] != 'DELETED' and 'deactivated' not in item
    }

    for href in hrefs:
        if href[1:] not in ids:
            ids[href[1:]] = {'is-deleted': True, 'is-closed': False}

    return ids


def get_friends(id_: int):
    response = query(
        url=f"https://api.vk.com/method/friends.get?user_id={id_}&v=5.124&fields=screen_name&access_token={os.environ['VK_TOKEN']}",
        as_json=True
    )
    return [
        item['screen_name']
        for item in response['response']['items']
        if item['first_name'] != 'DELETED' and 'deactivated' not in item
    ]


def get_communities(id_: int):
    try:
        response = query(
            url=f'https://vk.com/al_fans.php?act=box&al=1&al_ad=0&oid={id_}&tab=idols',
            as_vk_payload=True
        )
        bs = BeautifulSoup(response, features='html.parser')
        return [
            item['href'][1:]
            for item in bs.find_all('a', {'class': 'fans_idol_lnk'})
        ]
    except IndexError:
        return None
