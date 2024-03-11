from typing import Dict, Any

from api.services.abstract_service import AbstractService
from common.repositories.md_cota.quota_view import QuotaViewRepository
from common.exceptions import EntityNotFound
from api.schemas import BPMEventSchema
from simple_common.logger import logger
from common.clients.bpm import BPMClient


class BPMQuotasService(AbstractService):
    def __init__(self) -> None:
        self.__repository = QuotaViewRepository()
        self.__bpm_client = BPMClient()
        self.__logger = logger

    @property
    def _repository(self) -> QuotaViewRepository:
        return self.__repository

    def get_and_create(self, bpm_event: BPMEventSchema) -> Dict[str, Any]:
        quota_code = bpm_event.data.card.title
        self.__logger.debug(f"A buscar cota no banco, com quota_code: {quota_code}")
        quotas = self._repository.get_data_for_bpm([quota_code])

        if not quotas:
            message = (
                f"Cota com código {bpm_event.data.card.title}, recebida do pipefy, "
                f"não encontrada no banco do MD Cota."
            )
            self.__logger.error(message)
            raise EntityNotFound(detail=message)

        request_body = {"quotas": quotas}

        self.__logger.debug(
            f"Definido body para envio ao BPM, "
            f"a partir da cota recuperada:\n {request_body}"
        )
        self.__bpm_client.create_quota(request_body)

        return {"quota_code": quotas[0]["quota_code"]}
