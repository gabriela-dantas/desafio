from abc import abstractmethod
from fastapi_restful import Resource
from typing import List
from fastapi import Security

from api.auth.auth_bearer import api_key_header


class AbstractResource(Resource):
    def __init__(self) -> None:
        self._dependencies = [Security(api_key_header)]

    @abstractmethod
    def path(self) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def tags(self) -> List[str]:
        pass  # pragma: no cover

    @property
    def dependencies(self) -> List[Security]:
        return self._dependencies
