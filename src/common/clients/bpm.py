import os
import requests

from typing import Dict, Any, List
from fastapi import status, HTTPException
from fastapi.encoders import jsonable_encoder

from common.clients.abstract_client import AbstractClient
from simple_common.logger import logger
from common.exceptions import InternalServerError


class BPMClient(AbstractClient):
    def __init__(self) -> None:
        super().__init__()
        self.__logger = logger
        self.__md_quota_endpoint = "/api/internal/md_cotas/quotas"
        self.__santander_events_endpoint = "/api/internal/santander/events"

    @property
    def endpoint_url(self) -> str:
        return os.environ.get("BPM_WEBHOOK_URL")

    @property
    def headers(self) -> Dict[str, Any]:
        token = os.environ.get("BPM_WEBHOOK_TOKEN")

        return {"token": token}

    @property
    def timeout(self) -> int:
        return 10

    @property
    def url_create_quota(self) -> str:
        return f"{self.endpoint_url}{self.__md_quota_endpoint}"

    @property
    def santander_events_endpoint(self) -> str:
        return f"{self.endpoint_url}{self.__santander_events_endpoint}"

    def create_quota(self, body: Dict[str, List[dict]]) -> None:
        json_body = jsonable_encoder(body)

        request_data = {"json": json_body}

        response = self._make_request(
            self.url_create_quota, requests.post, **request_data
        )

        if response.status_code != status.HTTP_202_ACCEPTED:
            message = (
                f"Requisição para criar cota no BPM não retornou 202: "
                f"{response.status_code} - {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

    def update_santander_status(self, body: Dict[str, str]) -> int:
        request_data = {"json": body}

        try:
            response = self._make_request(
                self.santander_events_endpoint, requests.post, **request_data
            )
            return response.status_code
        except HTTPException as http_error:
            return http_error.status_code
