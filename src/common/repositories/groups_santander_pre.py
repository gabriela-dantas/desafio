from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import GroupsSantanderPreModel


class GroupsSantanderPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(GroupsSantanderPreModel)
