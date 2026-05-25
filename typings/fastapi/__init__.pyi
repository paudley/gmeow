from collections.abc import Callable
from typing import TypeVar

from _typeshed import Incomplete

_F = TypeVar("_F", bound=Callable[..., object])

class _State:
    def __getattr__(self, name: str) -> Incomplete: ...
    def __setattr__(self, name: str, value: Incomplete) -> None: ...

class _Router:
    routes: list[Incomplete]
    def lifespan_context(self, app: Incomplete) -> Incomplete: ...

class FastAPI:
    state: _State
    router: _Router
    def __init__(self, *args: Incomplete, **kwargs: Incomplete) -> None: ...
    def get(self, path: str, **kwargs: Incomplete) -> Callable[[_F], _F]: ...
    def post(self, path: str, **kwargs: Incomplete) -> Callable[[_F], _F]: ...
    def delete(self, path: str, **kwargs: Incomplete) -> Callable[[_F], _F]: ...
    def middleware(self, middleware_type: str) -> Callable[[_F], _F]: ...

class _HTTPExceptionError(Exception):
    status_code: int
    detail: Incomplete
    def __init__(self, status_code: int, detail: Incomplete = ...) -> None: ...

HTTPException = _HTTPExceptionError

class _Client:
    host: str

class _Headers:
    def get(self, key: str, default: str = ...) -> str: ...

class Request:
    app: FastAPI
    client: _Client
    headers: _Headers
    method: str
    url: Incomplete

class Response:
    def __init__(self, content: Incomplete = ..., status_code: int = ..., media_type: str = ..., **kwargs: Incomplete) -> None: ...
