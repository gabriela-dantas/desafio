from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import PrizeDrawSantanderPreModel


class PrizeDrawSantanderPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PrizeDrawSantanderPreModel)
