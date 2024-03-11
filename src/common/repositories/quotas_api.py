from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import QuotasAPIModel


class QuotasAPIRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotasAPIModel)
