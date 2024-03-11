from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import GroupsGMACModel


class GroupsGMACRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(GroupsGMACModel)
