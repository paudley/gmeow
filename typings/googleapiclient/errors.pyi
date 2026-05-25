class HttpError(Exception):
    status_code: int
    resp: object
    content: bytes
    def __init__(self, resp: object = ..., content: bytes = ..., uri: str = ...) -> None: ...
