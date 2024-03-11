from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import QuotasVolksPreModel


class QuotasVolksPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotasVolksPreModel)