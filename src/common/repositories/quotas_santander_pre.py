from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import QuotasSantanderPreModel


class QuotasSantanderPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotasSantanderPreModel)
