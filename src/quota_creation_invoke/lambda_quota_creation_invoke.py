from typing import List

from common.repositories.md_cota.quota_view import QuotaViewRepository
from common.event_schemas.quota_creation_event_schema import ExtractEventSchema
from simple_common.logger import logger
from common.clients.bpm import BPMClient
from common.exceptions.entity_not_found import EntityNotFound


class LambdaQuotaCreationInvoke:
    def __init__(self) -> None:
        self.__logger = logger
        self.__quota_view_repository = QuotaViewRepository()
        self.__bpm_client = BPMClient()

    def __verify_quotas(
        self, quotas: List[dict], event_data: ExtractEventSchema
    ) -> None:
        # Cotas Santander deverão ter enviado o share_id da Consorciei no evento, que deverá
        # ser enviado ao BPM. Além disso, para essas a referência externa deverá ser substituída
        # pelo número do contrato, segundo regra do BPM.
        if event_data.quota_code_list[0].share_id:
            self.__logger.debug(
                "Cotas Santander identificadas, adicionando share_id, e definindo "
                "número de contrato como referência externa, para envio ao BPM."
            )
            share_by_quota = {
                quota.quota_code: quota.share_id for quota in event_data.quota_code_list
            }
            for quota in quotas:
                quota["external_reference"] = quota["contract_number"]
                quota["share_id"] = share_by_quota[quota["quota_code"]]

    def invoke(self, event_data: ExtractEventSchema) -> None:
        self.__logger.debug("Buscando cotas para envio ao BPM.")

        quota_codes = list(
            map(
                lambda quota_schema: quota_schema.quota_code, event_data.quota_code_list
            )
        )

        quotas = self.__quota_view_repository.get_data_for_bpm(quota_codes)
        self.__verify_quotas(quotas, event_data)

        if not quotas:
            raise EntityNotFound(
                "Nenhuma cota encontrada no banco do MD Cota, para envio ao BPM."
            )

        request_body = {"quotas": quotas}
        self.__logger.debug(f"Body para envio ao BPM: {request_body}")
        self.__bpm_client.create_quota(request_body)
        self.__logger.debug("Cotas criadas com sucesso no BPM.")
