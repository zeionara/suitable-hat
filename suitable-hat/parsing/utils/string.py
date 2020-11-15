from ...converters import communities


def post_process_response(response: str):
    return response.replace('Expand text…', ' ').replace('Expand text...', ' ')


def make_community_ref(comminity_id: int):
    return communities[f'/wall-{comminity_id}']
