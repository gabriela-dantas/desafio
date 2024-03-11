from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import BeeReaderModel


class BeeReaderRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(BeeReaderModel)
