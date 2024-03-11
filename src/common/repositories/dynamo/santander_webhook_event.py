from common.repositories.dynamo.dynamo_abstract_repository import (
    DynamoAbstractRepository,
)


class SantanderWebhookEventRepository(DynamoAbstractRepository):
    @property
    def table_name(self) -> str:
        return "tb_md_cota_santander_webhook_event"
