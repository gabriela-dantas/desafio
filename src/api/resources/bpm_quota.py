from typing import List
from fastapi_restful import set_responses
from fastapi import status

from api.resources.abstract_resource import AbstractResource
from api.schemas import (
    BPMEventSchema,
    BPMQuotaCreateTimeoutError,
    BPMQuotaCreateNotFoundError,
    CreatedBPMQuotaSchema,
    BPMQuotaTooManyRedirectsError,
    BPMQuotaInternalError,
)
from api.services.bpm_quota import BPMQuotasService


class BPMQuotaResource(AbstractResource):
    def path(self) -> str:
        return "/bpm/quota"

    def tags(self) -> List[str]:
        return ["bpm_quota"]

    @set_responses(
        CreatedBPMQuotaSchema,
        status.HTTP_201_CREATED,
        responses={
            status.HTTP_404_NOT_FOUND: {"model": BPMQuotaCreateNotFoundError},
            status.HTTP_408_REQUEST_TIMEOUT: {"model": BPMQuotaCreateTimeoutError},
            status.HTTP_429_TOO_MANY_REQUESTS: {"model": BPMQuotaTooManyRedirectsError},
            status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": BPMQuotaInternalError},
        },
    )
    def post(self, bpm_event: BPMEventSchema):
        """
        Recebe dados de movimentação do card da cota no BPM, via Webhook, e caso seja OPT-IN,
        busca os dados da cota no Banco e os envia para criação da mesma na rota do BPM.
        """
        return BPMQuotasService().get_and_create(bpm_event)
