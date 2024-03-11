from common.repositories.abstract_repository import AbstractRepository
from common.models.md_quota import QuotaStatusModel


class QuotaStatusRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotaStatusModel)
