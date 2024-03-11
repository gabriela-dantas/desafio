from common.repositories.abstract_repository import AbstractRepository
from common.models.md_quota import QuotaHistoryDetailModel


class QuotaHistoryDetailRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotaHistoryDetailModel)
