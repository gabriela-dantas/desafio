from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import QuotasGMACPreModel


class QuotasGMACPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotasGMACPreModel)
