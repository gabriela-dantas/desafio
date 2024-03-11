from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import CustomersGMACPreModel


class CustomersGMACPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(CustomersGMACPreModel)
