import time

from abc import abstractmethod, ABCMeta
from typing import Any, Dict, Callable
from requests import Response
from requests.exceptions import HTTPError, Timeout, TooManyRedirects

from simple_common.logger import logger
from common.exceptions import (
    Timeout as TimeoutException,
    TooManyRedirects as TooManyRedirectsException,
    InternalServerError,
)


class AbstractClient(metaclass=ABCMeta):
    def __init__(self) -> None:
        self.__logger = logger

    @property
    @abstractmethod
    def endpoint_url(self) -> str:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def headers(self) -> Dict[str, Any]:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def timeout(self) -> int:
        pass  # pragma: no cover

    def _make_request(self, url: str, request_method: Callable, **kwargs) -> Response:
        try:
            self.__logger.info(
                f"Enviando requisição para "
                f"{request_method.__name__.upper()} {url}\n"
                f"Parâmetros: {kwargs}"
            )

            kwargs["headers"] = self.headers
            kwargs["timeout"] = self.timeout

            start = time.time()
            response: Response = request_method(url, **kwargs)
            end = time.time() - start

            self.__logger.info(
                f"Obtida resposta após {end} segundos. "
                f"status_code: {response.status_code} | content: {response.content}"
            )
            return response
        except Timeout:
            message = (
                f"Requisição para {url} gerou timeout após {self.timeout} segundos."
            )
            self.__logger.error(message)
            raise TimeoutException(message)
        except HTTPError as http_error:
            message = f"Requisição para {url} gerou erro HTTP: {http_error.args}"
            self.__logger.error(message)
            raise InternalServerError(message)
        except TooManyRedirects:
            message = f"Requisição para {url} excedeu redirecionamentos."
            self.__logger.error(message)
            raise TooManyRedirectsException(message)
        except Exception as exception:
            message = f"Requisição para {url} gerou erro desconhecido: {exception}."
            self.__logger.error(message)
            raise InternalServerError(message)
