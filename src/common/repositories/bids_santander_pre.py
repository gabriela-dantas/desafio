from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import BidsSantanderPreModel


class BidsSantanderPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(BidsSantanderPreModel)
