import json
import boto3
import os
from typing import Dict, List

from botocore.exceptions import ClientError
from unidecode import unidecode

from api.services.abstract_service import AbstractService
from api.constants import type_etl
from common.repositories.abstract_repository import AbstractRepository
from common.repositories.quotas_api import QuotasAPIRepository
from simple_common.logger import logger
from common.exceptions import UnprocessableEntity, InternalServerError
from api.schemas import QuotasAPICreateSchema


class QuotasAPIService(AbstractService):
    def __init__(self) -> None:
        self.__repository = QuotasAPIRepository()
        self.__creation_limit_size = 512
        self.__logger = logger
        self.__put_event_bridge = boto3.client("events")

    @property
    def _repository(self) -> AbstractRepository:
        return self.__repository

    @property
    def creation_limit_size(self) -> int:
        return self.__creation_limit_size

    def __verify_limit(self, quotas: List[dict]) -> None:
        if len(quotas) > self.__creation_limit_size:
            raise UnprocessableEntity(
                f"Lista de cotas a serem criadas ultrapassou "
                f"o limite de {self.__creation_limit_size} unidades."
            )

    def create_many(self, quotas_api: QuotasAPICreateSchema) -> Dict[str, str]:
        self.__verify_limit(quotas_api.quotas)

        quotas_api.administrator = unidecode(quotas_api.administrator.upper())
        lista_id = []

        for quota_data in quotas_api.quotas:
            quota_data = json.dumps(quota_data)

            target_quotas = {
                "request_body": quota_data,
                "administrator": quotas_api.administrator,
                "endpoint_generator": quotas_api.endpoint_generator,
            }

            quota = self._repository.create(target_quotas)
            lista_id.append(quota.id_quotas_itau)

        self.put_event(lista_id, quotas_api.endpoint_generator)
        return {"message": f"Volume de {len(lista_id)} entidades inserido com sucesso!"}

    def put_event(self, list_id: list, endpoint_generator: str) -> None:
        self.__logger.info("Iniciando criação do evento")
        event_bus_name = os.environ["EVENT_BUS_NAME_START_ETL"]
        event_detail_type = type_etl[endpoint_generator]
        entry = {
            "Source": "lambda",
            "DetailType": event_detail_type,
            "Detail": json.dumps({"quota_id_list": list_id}),
            "EventBusName": event_bus_name,
        }

        try:
            response = self.__put_event_bridge.put_events(Entries=[entry])
            self.__logger.debug(f"Resposta da publicação: {response}. evento:{entry}")
        except ClientError as client_error:
            internal_error = InternalServerError(
                detail=f"Comportamento inesperado ao tentar publicar o evento.{client_error}",
            )
            raise internal_error
