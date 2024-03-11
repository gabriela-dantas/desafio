import os
import requests

from typing import Any, Dict
from fastapi import status

from common.clients.abstract_client import AbstractClient
from simple_common.logger import logger
from common.exceptions import InternalServerError


class CubeesClient(AbstractClient):
    def __init__(self) -> None:
        super().__init__()
        self.__logger = logger

    @property
    def endpoint_url(self) -> str:
        return os.environ.get("CUBEES_URL")

    @property
    def headers(self) -> Dict[str, Any]:
        api_key = os.environ.get("CUBEES_API_KEY")

        return {"x-api-key": api_key}

    @property
    def timeout(self) -> int:
        return 30

    @property
    def create_customer_url(self) -> str:
        return f"{self.endpoint_url}/customer"

    @property
    def create_related_person_url(self) -> str:
        return f"{self.endpoint_url}/customer/related-person"

    def create_customer(self, customer_data: Dict[str, Any]) -> str:
        request_data = {"json": customer_data}
        response = self._make_request(
            self.create_customer_url, requests.post, **request_data
        )

        if response.status_code != status.HTTP_201_CREATED:
            message = (
                f"Requisição para criar cliente no Cubees não retornou 201: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.exception(message)
            raise InternalServerError(message)

        data = response.json()
        self.__logger.info(f"Resposta do Cubees: {data}")
        try:
            return data["person_code"]
        except KeyError:
            message = (
                f"Request ao Cubees não retornou person code: "
                f"{response.status_code} - {data}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

    def related_person(
        self, person_bond: Dict[str, Any], legal_person: Dict[str, str]
    ) -> None:
        request_data = {"json": person_bond}
        params = {"params": legal_person}
        response = self._make_request(
            self.create_related_person_url, requests.post, **request_data, **params
        )

        if response.status_code != status.HTTP_201_CREATED:
            message = (
                f"Requisição para relacionar cliente no Cubees não retornou 201: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        data = response.json()
        self.__logger.info(f"Resposta do Cubees: {data}")

    def get_customer(self, person_ext_code: Dict[str, Any]) -> Dict[str, Any]:
        params = {"params": person_ext_code}
        response = self._make_request(self.create_customer_url, requests.get, **params)
        if response.status_code != status.HTTP_200_OK:
            message = (
                f"Requisição para criar cliente no Cubees não retornou 200: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        data = response.json()
        self.__logger.info(f"Resposta do Cubees: {data}")
        return data
