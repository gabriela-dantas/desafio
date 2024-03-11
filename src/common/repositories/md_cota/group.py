from common.repositories.abstract_repository import AbstractRepository
from common.models.md_quota import GroupModel


class GroupRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(GroupModel)
