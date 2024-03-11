import os
import requests

from typing import Dict, Any
from fastapi import status

from common.clients.abstract_client import AbstractClient
from simple_common.logger import logger
from common.exceptions import InternalServerError


class ConsorcieiClient(AbstractClient):
    def __init__(self) -> None:
        super().__init__()
        self.__logger = logger
        self.__quota_detail_endpoint = "/secondary/shares/{share_id}"
        self.__company_detail_endpoint = "/secondary/shares/{share_id}/companies"
        self.__representatives_endpoint = "/secondary/shares/{share_id}/signers"
        self.__send_link_proof_life = (
            "/secondary/shares/{share_id}/signers/{signer_id}/liveness"
        )

    @property
    def endpoint_url(self) -> str:
        return os.environ.get("CONSORCIEI_API_URL")

    @property
    def headers(self) -> Dict[str, Any]:
        token = os.environ.get("CONSORCIEI_API_TOKEN")

        return {"Authorization": token}

    @property
    def timeout(self) -> int:
        return 60

    @property
    def send_link_proof_life_endpoint(self) -> str:
        return f"{self.endpoint_url}{self.__send_link_proof_life}"

    @property
    def quota_detail_endpoint(self) -> str:
        return f"{self.endpoint_url}{self.__quota_detail_endpoint}"

    @property
    def company_detail_endpoint(self) -> str:
        return f"{self.endpoint_url}{self.__company_detail_endpoint}"

    @property
    def representatives_detail_endpoint(self) -> str:
        return f"{self.endpoint_url}{self.__representatives_endpoint}"

    def get_quota_details(self, share_id: str) -> Dict[str, Any]:
        self.__logger.info("Buscando cota na API da consorciei...")
        param = {"share_id": share_id}

        response = self._make_request(
            self.quota_detail_endpoint.format(**param), requests.get
        )

        if response.status_code != status.HTTP_200_OK:
            message = (
                f"Requisição para obter cota não retornou 200: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        quota = response.json()
        self.__logger.info(f"Cota obtida da API: {quota}")
        return quota

    def get_company_details(self, share_id: str) -> Dict[str, Any]:
        self.__logger.info("Buscando detalhes empresa na API da consorciei...")
        param = {"share_id": share_id}

        response = self._make_request(
            self.company_detail_endpoint.format(**param), requests.get
        )

        if response.status_code != status.HTTP_200_OK:
            message = (
                f"Requisição para obter detalhes da empresa não retornou 200: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        company = response.json()
        self.__logger.info(f"Empresa obtida da API: {company}")
        return company

    def get_representatives(self, share_id: str) -> Dict[str, Any]:
        self.__logger.info("Buscando lista de representantes na API da consorciei...")
        param = {"share_id": share_id}

        response = self._make_request(
            self.representatives_detail_endpoint.format(**param), requests.get
        )

        if response.status_code != status.HTTP_200_OK:
            message = (
                f"Requisição para obter lista de representantes não retornou 200: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        representatives = response.json()
        self.__logger.info(f"Representantes obtidos da API: {representatives}")
        return representatives

    # conferir se vai substituir certo
    def send_link_proof_life(
        self, share_id: str, signed_id: str, magic_link: Dict[str, Any]
    ) -> None:
        self.__logger.info("Enviando link de prova de vida na API da consorciei...")
        request_data = {"json": magic_link}
        param = {"share_id": share_id, "signer_id": signed_id}

        response = self._make_request(
            self.send_link_proof_life_endpoint.format(**param),
            requests.post,
            **request_data,
        )
        if response.status_code != status.HTTP_200_OK:
            message = (
                f"Requisição para enviar link de prova de vida não retornou 201: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        self.__logger.info(
            f"Resposta da API da consorciei para o envio de link: {response.status_code}"
        )
