from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import ContempladosItauModel


class ContempladosItauRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(ContempladosItauModel)
