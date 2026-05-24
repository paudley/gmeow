from _typeshed import Incomplete

class Response:
    def __init__(self, content: Incomplete = ..., status_code: int = ..., media_type: str = ..., **kwargs: Incomplete) -> None: ...

class FileResponse(Response):
    def __init__(self, path: Incomplete, **kwargs: Incomplete) -> None: ...
