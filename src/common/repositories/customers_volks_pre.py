from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import CustomersVolksPreModel


class CustomersVolksPrePreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(CustomersVolksPreModel)
