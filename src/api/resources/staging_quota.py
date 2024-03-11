from typing import List
from fastapi_restful import set_responses
from fastapi import status

from api.resources.abstract_resource import AbstractResource
from api.schemas.staging_quota import (
    QuotasAPIBatchSchema,
    QuotasAPICreateSchema,
    QuotasBatchConflictError,
    QuotasBatchInternalError,
)
from api.services.quotas_api import QuotasAPIService


class StagingQuotaResource(AbstractResource):
    def path(self) -> str:
        return "/staging/quota"

    def tags(self) -> List[str]:
        return ["staging_quota"]

    @set_responses(
        QuotasAPIBatchSchema,
        status.HTTP_201_CREATED,
        responses={
            status.HTTP_409_CONFLICT: {"model": QuotasBatchConflictError},
            status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": QuotasBatchInternalError},
        },
    )
    def post(self, quotas_api: QuotasAPICreateSchema):
        """
        Recebe os dados brutos de cotas via BPM, oriundos de variadas ADMs e rotas.
        """
        return QuotasAPIService().create_many(quotas_api)
