from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import BidsVolksPreModel


class BidsVolksPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(BidsVolksPreModel)
