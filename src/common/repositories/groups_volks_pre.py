from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import GroupsVolksPreModel


class GroupsVolksPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(GroupsVolksPreModel)
