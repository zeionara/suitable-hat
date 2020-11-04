def post_process_response(response: str):
    return response.replace('Expand text…', ' ').replace('Expand text...', ' ')


def make_community_ref(comminity_id: int):
    return f'/wall-{comminity_id}'
